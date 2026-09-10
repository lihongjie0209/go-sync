# Windows Service 与滚动日志

发布包原生支持 Windows Service Control Manager，不需要 NSSM 或额外脚本。请使用管理员 PowerShell，并为每个采集实例使用唯一的服务名、`source_id`、`data_dir` 和日志文件。

## 配置

Service 模式的 `data_dir` 必须是绝对路径。`log.file` 可以是绝对路径；相对路径以配置文件所在目录为基准。省略 `log.file` 时，Service 默认写入配置目录下的 `logs/go-sync.log`。

```json
{
  "data_dir": "D:\\go-sync\\data\\client-01",
  "log": {
    "file": "D:\\go-sync\\logs\\client-01.log",
    "level": "info",
    "max_size_mb": 100,
    "max_backups": 10,
    "max_age_days": 30,
    "compress": true
  }
}
```

- `level`：`debug`、`info`、`warn` 或 `error`。
- `max_size_mb`：单个日志文件达到该大小后滚动。
- `max_backups`：保留的旧日志文件数量；`0` 表示不按数量删除。
- `max_age_days`：旧日志保留天数；`0` 表示不按天数删除。
- `compress`：是否用 gzip 压缩滚动后的日志。

前台执行 `run` 时，日志会写入 stderr；配置了 `log.file` 时也会同时写入滚动文件。Service 模式只写日志文件。日志是每行一个 JSON 对象，可以直接由日志采集器读取。

## 安装与管理

```powershell
Set-Location 'D:\go-sync'

.\go-sync.exe check --config 'D:\go-sync\config.json'
.\go-sync.exe service install --name 'go-sync-client-01' --display-name 'Go Sync Client 01' --config 'D:\go-sync\config.json'
.\go-sync.exe service start --name 'go-sync-client-01'
.\go-sync.exe service status --name 'go-sync-client-01'
.\go-sync.exe service stop --name 'go-sync-client-01'
.\go-sync.exe service uninstall --name 'go-sync-client-01'
```

服务启动类型为“自动”。`uninstall` 会先请求服务正常停止，最多等待 30 秒，然后删除服务注册；不会删除配置、日志、本地队列或源数据库对象。

安装后的可执行文件路径和配置路径由 SCM 固定保存，因此安装后不要移动它们。配置中的 `${ENV_NAME}` 在服务进程账号环境中展开：使用机器级环境变量，或者在“服务”中改为拥有相应环境和文件权限的专用低权限账号。不要把数据库密码或 HTTP Token 直接写入日志或命令参数。
