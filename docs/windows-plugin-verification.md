# Windows 插件交付验证（2026-09-06）

## GitHub CI

- 公共仓库：https://github.com/lihongjie0209/wal2json-windows
- 发布：https://github.com/lihongjie0209/wal2json-windows/releases/tag/wal2json_2_6
- 完整矩阵：https://github.com/lihongjie0209/wal2json-windows/actions/runs/34035364367
- 最终结果：156 个目标 verified、0 missing；两项临时服务器启动失败在独立复测和失败任务重跑后通过。
- 范围：PG 9.4.0–9.4.26、9.5.0–9.5.25、9.6.0–9.6.24，各 x86/x64。
- 每个目标使用 EDB 对应完整版本的头文件/import library 编译，并使用同一个包中的 Windows postgres.exe 启动合成数据临时集群验证。
- 断言：LOAD、slot 创建/清理、INSERT/UPDATE/DELETE 内容、format 1 非零 nextlsn；2.6 另验证 format 2 B/I/C 边界、B/C LSN 一致及 nextlsn > commit LSN。
- 缺少依赖只允许明确 HTTP 404/410 跳过，网络/权限/编译/测试错误阻止发布。覆盖清单、观察到的 EDB SHA256 和每个 DLL 的来源均随发布记录。

这不是所有 CDC 恢复场景的完整验证，也不表示所有历史 Windows 操作系统均兼容。

## 用户 PG 副本

使用提供的 PostgreSQL 9.5.2、Visual C++ 1800、32-bit 程序，未启动原业务数据，另外初始化临时数据库。

发现并规避了一个真实的 ABI 问题：PG 9.5.25 编译的 DLL 在 9.5.2 上可以加载，但事务 nextlsn 输出 0/0。PG 9.5.2 专用 CI 发布 DLL 正常，因此内嵌安装采用精确补丁版本匹配。

专用 DLL 校验和：`30f04459c5199974f67a64cecaeee4207a569b28131d31265390794b2242face`。

插件独立测试通过：LOAD、逻辑 slot、format 2、type OID、numeric-data-types-as-string、无主键 REPLICA IDENTITY FULL、重复全 NULL 行、bytea、DML 数量和有效 LSN。

采集器本机安装验证：Windows amd64 和 386 采集器分别连接 x86 PG 9.5.2，目标 DLL 缺失时自动从内嵌发布包安装；快照完成，增量 durable LSN 从 `0/1712C48` 推进到 `0/1712CF8`，Prometheus 接口正常。实际 Windows 下载目录存在路径重定向，配置路径核验采用文件身份比较而非字符串匹配。

测试 HTTP 地址刻意不可用，只检查本地持久化和重试，不把这次测试作为接收端 ACK/去重通过的证据。进程与临时数据库清理，测试前的 DLL 恢复；原备份未改动。

## Go 检查

- `go test -race ./...`：通过。
- `go vet ./...`：通过。
- Windows amd64/386 构建：通过。
- Windows amd64/386 原生插件单元测试：通过，覆盖 156 个内嵌 ZIP 的 SHA256、manifest、PE 位数及 DLL 校验和，以及不覆盖、清理、损坏包拒绝等行为。
- `go test -tags=integration -run '^$' ./...`：仅确认集成测试编译通过，未把它算作运行集成测试。

采集器原有 Testcontainers 集成测试组织不变，本次没有改为手动 Docker。Windows DLL 兼容与用户副本验收使用原生 Windows 进程，Linux 容器无法替代这种验证。此前 `docs/verification.md` 中记录的基础依赖安全扫描问题不因本次功能验证而消失。
