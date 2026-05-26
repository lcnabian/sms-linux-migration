// SPDX-License-Identifier: GPL-2.0-only
/*
 * Minimal dattobd COW changed-block lister.
 *
 * It reads an old COW file produced by transition-to-snapshot and prints the
 * blocks that changed since the previous snapshot. The output can be consumed
 * by scripts that read those offsets from /dev/dattoN and write them to a
 * target block device or image.
 */

#define _FILE_OFFSET_BITS 64

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <linux/fs.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <unistd.h>

#define COW_UUID_SIZE 16
#define COW_BLOCK_LOG_SIZE 12
#define COW_BLOCK_SIZE (1ULL << COW_BLOCK_LOG_SIZE)
#define COW_HEADER_SIZE 4096
#define COW_MAGIC ((uint32_t)4776)
#define INDEX_BUFFER_SIZE 8192

#define MIN(a, b) ((a) < (b) ? (a) : (b))

struct cow_header {
	uint32_t magic;
	uint32_t flags;
	uint64_t fpos;
	uint64_t fsize;
	uint64_t seqid;
	uint8_t uuid[COW_UUID_SIZE];
	uint64_t version;
	uint64_t nr_changed_blocks;
};

enum output_mode {
	OUTPUT_BLOCKS,
	OUTPUT_RANGES,
};

static void usage(const char *prog)
{
	fprintf(stderr,
		"Usage: %s [--ranges] <cow-file> <snapshot-or-base-device>\n"
		"\n"
		"Default output: block,offset,length\n"
		"--ranges output: start_block,offset,length,blocks\n",
		prog);
}

static int get_size_bytes(int fd, uint64_t *size)
{
	unsigned long long blk_size = 0;
	off_t end;

	if (ioctl(fd, BLKGETSIZE64, &blk_size) == 0) {
		*size = blk_size;
		return 0;
	}

	end = lseek(fd, 0, SEEK_END);
	if (end < 0)
		return -1;

	*size = (uint64_t)end;
	return 0;
}

static ssize_t read_full(int fd, void *buf, size_t len, off_t off)
{
	char *p = buf;
	size_t done = 0;

	while (done < len) {
		ssize_t n = pread(fd, p + done, len - done, off + (off_t)done);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (n == 0)
			break;
		done += (size_t)n;
	}

	return (ssize_t)done;
}

static void emit_range(uint64_t start_block, uint64_t blocks)
{
	if (!blocks)
		return;

	printf("%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",
	       start_block,
	       start_block * COW_BLOCK_SIZE,
	       blocks * COW_BLOCK_SIZE,
	       blocks);
}

int main(int argc, char **argv)
{
	enum output_mode mode = OUTPUT_BLOCKS;
	const char *cow_path;
	const char *dev_path;
	int cow_fd = -1, dev_fd = -1;
	struct cow_header header;
	uint64_t dev_size, total_blocks, total_chunks;
	uint64_t blocks_done = 0, count = 0;
	uint64_t range_start = 0, range_len = 0;
	uint64_t *mappings = NULL;
	int argi = 1;

	if (argc > 1 && strcmp(argv[argi], "--ranges") == 0) {
		mode = OUTPUT_RANGES;
		argi++;
	}

	if (argc - argi != 2) {
		usage(argv[0]);
		return 2;
	}

	cow_path = argv[argi];
	dev_path = argv[argi + 1];

	cow_fd = open(cow_path, O_RDONLY);
	if (cow_fd < 0) {
		perror("open cow file");
		return 1;
	}

	dev_fd = open(dev_path, O_RDONLY);
	if (dev_fd < 0) {
		perror("open snapshot/base device");
		close(cow_fd);
		return 1;
	}

	if (read_full(cow_fd, &header, sizeof(header), 0) != (ssize_t)sizeof(header)) {
		perror("read cow header");
		close(dev_fd);
		close(cow_fd);
		return 1;
	}

	if (header.magic != COW_MAGIC) {
		fprintf(stderr, "invalid dattobd COW magic: got %" PRIu32 "\n", header.magic);
		close(dev_fd);
		close(cow_fd);
		return 1;
	}

	if (get_size_bytes(dev_fd, &dev_size) != 0) {
		perror("get device size");
		close(dev_fd);
		close(cow_fd);
		return 1;
	}

	total_blocks = (dev_size + COW_BLOCK_SIZE - 1) / COW_BLOCK_SIZE;
	total_chunks = (total_blocks + INDEX_BUFFER_SIZE - 1) / INDEX_BUFFER_SIZE;

	mappings = calloc(INDEX_BUFFER_SIZE, sizeof(uint64_t));
	if (!mappings) {
		perror("calloc mappings");
		close(dev_fd);
		close(cow_fd);
		return 1;
	}

	for (uint64_t i = 0; i < total_chunks; i++) {
		uint64_t blocks_to_read = MIN((uint64_t)INDEX_BUFFER_SIZE,
					      total_blocks - blocks_done);
		size_t bytes_to_read = (size_t)(blocks_to_read * sizeof(uint64_t));
		off_t off = COW_HEADER_SIZE +
			    (off_t)(INDEX_BUFFER_SIZE * sizeof(uint64_t) * i);

		if (read_full(cow_fd, mappings, bytes_to_read, off) !=
		    (ssize_t)bytes_to_read) {
			perror("read cow mappings");
			free(mappings);
			close(dev_fd);
			close(cow_fd);
			return 1;
		}

		for (uint64_t j = 0; j < blocks_to_read; j++) {
			uint64_t block = (INDEX_BUFFER_SIZE * i) + j;
			int changed = mappings[j] != 0;

			if (mode == OUTPUT_BLOCKS) {
				if (changed) {
					printf("%" PRIu64 ",%" PRIu64 ",%u\n",
					       block,
					       block * COW_BLOCK_SIZE,
					       (unsigned)COW_BLOCK_SIZE);
					count++;
				}
				continue;
			}

			if (changed) {
				if (!range_len)
					range_start = block;
				range_len++;
				count++;
			} else if (range_len) {
				emit_range(range_start, range_len);
				range_len = 0;
			}
		}

		blocks_done += blocks_to_read;
	}

	if (mode == OUTPUT_RANGES && range_len)
		emit_range(range_start, range_len);

	fprintf(stderr,
		"cow_seqid=%" PRIu64 " changed_blocks=%" PRIu64
		" header_nr_changed_blocks=%" PRIu64 "\n",
		header.seqid, count, header.nr_changed_blocks);

	free(mappings);
	close(dev_fd);
	close(cow_fd);
	return 0;
}
