# dattobd dd migration prototype

This is a minimal prototype for Linux block-level migration with `dattobd`.

It does three things:

1. Create a point-in-time snapshot and copy it fully with `dd`.
2. List changed block ranges from a previous dattobd COW file.
3. Apply only those changed ranges from `/dev/dattoN` to a target file or block device with `dd`.
4. Optionally transfer full data or changed ranges over a TCP socket.

## Build

On the Linux source machine:

```bash
cc -O2 -Wall -Wextra -o list-changed-blocks list-changed-blocks.c
chmod +x dattobd-dd-migrate.sh
chmod +x block_socket.py
```

## Flow

Install and load dattobd first:

```bash
modprobe dattobd
```

First full copy:

```bash
MINOR=0 ./dattobd-dd-migrate.sh full /dev/sda1 /.datto0 /dev/target
MINOR=0 ./dattobd-dd-migrate.sh begin-incremental
```

Later, create a new snapshot and inspect changed ranges:

```bash
MINOR=0 ./dattobd-dd-migrate.sh next-snapshot /.datto1
MINOR=0 ./dattobd-dd-migrate.sh list /.datto0 /dev/datto0
```

Apply only changed ranges to the target:

```bash
MINOR=0 ./dattobd-dd-migrate.sh apply-incremental /.datto0 /dev/datto0 /dev/target
```

Then return to incremental tracking:

```bash
MINOR=0 ./dattobd-dd-migrate.sh begin-incremental
```

For the next cycle, `/.datto1` becomes the old COW file, and you create a new one such as `/.datto2`.

## Socket transport

Start a receiver on the target side:

```bash
./block_socket.py receive --host 0.0.0.0 --port 19090 --target /dev/vdb1
```

Send a full copy from the source side:

```bash
./block_socket.py send-full --host TARGET_IP --port 19090 --source /dev/datto0
```

For an incremental copy, first create ranges:

```bash
./list-changed-blocks --ranges /.datto0 /dev/datto0 > ranges.csv
```

Then start the receiver again and send only those ranges:

```bash
./block_socket.py receive --host 0.0.0.0 --port 19090 --target /dev/vdb1
./block_socket.py send-ranges --host TARGET_IP --port 19090 --source /dev/datto0 --ranges ranges.csv
```

The receiver writes each chunk at the explicit byte offset supplied by the sender. Each chunk is protected by a SHA-256 checksum over the transferred bytes.

## Important notes

- Run only on a disposable test disk first.
- The target must be at least as large as the source block device.
- `dd` writes by block offset, so source and target layouts must match.
- This handles block data only. Partition tables, boot repair, filesystem UUID conflicts, fstab, initramfs, and grub are separate migration concerns.
- Stop applications or freeze filesystems for the final cutover if you need application-consistent data.
