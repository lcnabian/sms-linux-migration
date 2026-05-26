# dattobd 块级迁移原型说明

## 目录

- `migration-ui/`：本地 Web 控制台，负责填写源端/目标端 SSH、推送工具、启动全量和增量同步。
- `dattobd-migrate-tools/`：远端执行工具。
  - `block_socket.py`：socket 全量/增量块传输。
  - `list-changed-blocks.c`：读取 dattobd COW 文件，列出变更块范围。
  - `dattobd-dd-migrate.sh`：早期 shell 验证脚本。
- `dattobd-inspect/`：dattobd 源码和 `dbdctl`，用于编译内核模块并操作快照。

## 推荐运行方式

在控制端运行：

```bash
python migration-ui/server.py
```

然后打开：

```text
http://127.0.0.1:8765/
```

源端设备请填写已经挂载的文件系统块设备，例如：

```text
/dev/vda1
```

目标端可以填写整盘或分区：

```text
/dev/vdb
/dev/vdb1
```

如果目标端填写整盘，迁移结果会写入整盘起始位置，目标盘可能表现为“裸文件系统”，不一定有分区表。若需要标准分区结构，请先创建目标分区，再写入 `/dev/vdb1`。

## 是否必须运行时编译

`dattobd.ko` 是 Linux 内核模块，和以下内容强相关：

- 当前运行内核版本：`uname -r`
- 当前内核头文件/构建目录：`/lib/modules/$(uname -r)/build`
- 编译器、内核配置、符号版本

因此它不是一个可以随便跨机器复用的普通二进制。最稳妥方式是在源端运行时编译。

可以手动提前编译，但必须满足：

1. 编译机器和源端机器的 `uname -r` 一致。
2. 内核 headers/devel 包一致。
3. 编译出的 `src/dattobd.ko`、`app/dbdctl`、`lib/` 一起拷到源端。
4. 目标源端能成功执行 `insmod src/dattobd.ko`。

如果是一批相同镜像、相同内核的机器，可以预编译一次，然后复用同一份产物。只要内核升级或换发行版，建议重新编译。

`block_socket.py` 和 `list-changed-blocks` 不像内核模块那么敏感：

- `block_socket.py` 只依赖 Python 3。
- `list-changed-blocks` 是普通用户态 C 程序，通常可以在目标 Linux 上编译一次复用，但为了兼容性也建议远端编译。

## 手动运行的大致流程

源端：

```bash
cd /root/dattobd-ui-work/dattobd-inspect
make
insmod src/dattobd.ko
export LD_LIBRARY_PATH=$PWD/lib:$LD_LIBRARY_PATH
app/dbdctl setup-snapshot /dev/vda1 /.datto-cow-0 0
```

目标端：

```bash
cd /root/dattobd-ui-work/dattobd-migrate-tools
./block_socket.py receive --host 0.0.0.0 --port 19090 --target /dev/vdb
```

源端全量：

```bash
cd /root/dattobd-ui-work/dattobd-migrate-tools
./block_socket.py send-full --host <target-ip> --port 19090 --source /dev/datto0
```

源端进入增量追踪：

```bash
cd /root/dattobd-ui-work/dattobd-inspect
app/dbdctl transition-to-incremental 0
```

下一轮增量：

```bash
cd /root/dattobd-ui-work/dattobd-inspect
app/dbdctl transition-to-snapshot /.datto-cow-1 0

cd /root/dattobd-ui-work/dattobd-migrate-tools
./list-changed-blocks --source /dev/datto0 --cow /.datto-cow-0 > ranges.csv
./block_socket.py send-ranges --host <target-ip> --port 19090 --source /dev/datto0 --ranges ranges.csv

cd /root/dattobd-ui-work/dattobd-inspect
app/dbdctl transition-to-incremental 0
```

## 一致性校验

全量后可按块设备计算 hash：

```bash
sha256sum /dev/datto0
sha256sum /dev/vdb
```

如果目标端是整盘但源端是分区，需要只比较源端大小范围。更安全的做法是按固定大小切块比较，或者在业务停写后对同等长度执行 `cmp -n <source-size>`。

## 注意事项

- 源端不能填未挂载整盘 `/dev/vda`，dattobd 会报 `illegal to perform setup while unmounted`。
- 目标端写入会覆盖数据，务必确认目标盘没有重要内容。
- 该原型适合验证块级迁移逻辑，不建议直接用于生产。
