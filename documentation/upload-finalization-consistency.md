# 上传收尾一致性：完成 / 放弃 / 清理

本文档说明分片直传的三个收尾入口（`POST /video/{id}/complete`、
`POST /video/{id}/abort`、后台 `cleanupAbandonedUploads`）现在遵循的一致语义，
以及如何在**没有真实云凭据**的情况下验证对象存储调用序列与并发时序。

## 1. 核心不变量

同一条 `videos` 记录与它在对象存储中的源对象，在任意并发/失败组合下最终满足：

1. **记录推进 ⇒ 对象存在且不会被这三个入口删除。**
   状态一旦从 `0 待上传` 条件更新为 `1 已落库`，完成接口幂等返回 `202`，
   放弃接口幂等返回 `204`，清理扫描不再选中该行。
2. **记录删除 ⇒ 分片上传已 Abort、不存在已合并的最终对象。**
   放弃/清理在删除行之前必须先做对象对账，发现“已合并但状态未推进”时改为
   **收养（adopt）**：把状态推进到 `1`，保留对象，而不是删除。
3. **合并成功之后的任何失败，都留下可接手的中间态**：
   行仍是 `status=0`，最终对象已存在。客户端重试 `complete` 或下一轮清理会
   通过 `ObjectSize` 对账发现该对象并推进状态，**不会第二次调用
   `CompleteMultipartUpload`**。
4. **列表可见性单调。** `GET /video/` 只查询 `status <> 0`；状态只允许
   `0 → 1` 单向推进，因此视频一旦出现就不会在这三个入口的并发下消失（无闪回）。

## 2. 串行化与对账协议

- 三个入口都通过 `utils.AcquireVideoLock` 获取同一个 PostgreSQL
  **咨询锁** `pg_advisory_lock(hashtextextended(video_id, 0))`，
  并在锁内用带 `status=0`（清理还带过期时间谓词）的 SQL **重读权威状态**。
  锁保证“存储副作用 + 状态/行变更”临界区对同一 video 串行。
- 所有破坏性操作之前先调用 `inspectSourceObject`：

  | 对象状态 | 含义 | 处理 |
  | --- | --- | --- |
  | `objectAbsent` | 没有最终对象 | 正常 ListParts→合并，或 Abort/Delete/删行 |
  | `objectMatches` | 最终对象大小与 `source_file_size` 一致 | 直接条件推进 `0→1`（收养），**不再合并/删除** |
  | `objectMismatch` | 同 key 对象大小不符 | 判定为无效对象，删除对象与待上传行 |
  | 查询报错（非 NotFound） | 状态未知 | **中止本次操作并保留现场**，等重试/下轮清理，绝不盲删 |

- 状态提交使用条件更新
  `UPDATE videos SET status=1,... WHERE video_id=$n AND status=0 [AND user_id=...]`，
  只有 `RowsAffected==1` 的调用方触发后续转码任务，避免重复处理。
- 放弃接口在“行已不是待上传”时直接 `204`，**零存储调用**（A7）。
- 清理对 `AbortMultipartUpload` 返回的 `NoSuchUpload`/
  `storage.ErrUploadNotFound` 保持容忍（中止一个已完成/已不存在的分片上传
  本来就是幂等目标）。

## 3. 可替换的存储客户端（无云凭据验证调用序列）

存储副作用统一收敛到接口 `storage.UploadStore`
（`storage/upload_store.go`），生产实现是 `storage.S3UploadStore`
（真实 aws-sdk-go-v2 适配器，`storage/s3_upload_store.go`）。
控制器通过可替换的包级工厂获取它：

```go
controllers.UploadStoreFactory = func(ctx) (storage.UploadStore, error) { ... }
```

测试中把工厂替换为 `controllers.RecordingStore`
（`controllers/recording_store_test.go`）：一个带互斥锁、记录完整调用序列、
可设屏障（barrier）和注入错误的内存对象存储。于是可以精确断言，例如：

- 重复完成时 `CompleteMultipartUpload` 全生命周期只调用 **1** 次；
- 完成胜出的竞态里 `DeleteObject` 调用 **0** 次；
- 放弃胜出时 `CompleteMultipartUpload` **0** 次、`AbortMultipartUpload` **1** 次；
- 清理收养已合并对象时 `DeleteObject`/`AbortMultipartUpload` 均为 **0** 次；
- 清理重复执行不产生新增调用（幂等）。

此外 `storage/s3_upload_store_test.go` 用 `httptest.Server` 模拟 S3 的 XML/HTTP
端点（ListObjectsV2、ListParts、CompleteMultipartUpload、Abort/Delete、
NoSuchUpload 错误体），让**真实 AWS SDK 适配器**在假端点上跑调用序列与错误
映射，同样不需要任何云凭据。

也可以对接本地 MinIO 做更接近生产的验证：

```bash
docker run -d -p 9000:9000 -e MINIO_ROOT_USER=dekho -e MINIO_ROOT_PASSWORD=dekho12345 minio/minio server /data
# 然后在 .env 中设置
# APPWRITE_S3_ENDPOINT=http://127.0.0.1:9000
# APPWRITE_PROJECT_ID=dekho
# APPWRITE_KEY=dekho12345
# BUCKET_ID=dekho   （先在 MinIO 中建桶）
```

## 4. 自动化测试与场景映射

测试需要**真实 PostgreSQL**（咨询锁、条件更新、触发器故障注入都依赖真实数据库），
对象存储则全部为替身。

| 验收项 | 测试 |
| --- | --- |
| A1 完成幂等、不重复合并 | `TestComplete_IdempotentDuplicateSubmission`、`TestComplete_AlreadyAdvanced_NoStorageCalls` |
| A2 完成/放弃并发（两种锁顺序 + 12 轮压力） | `TestConcurrent_CompleteVsAbort_CompleteWins`、`..._AbortWins`、`..._StressInvariant` |
| A3 清理遇到已合并对象改为收养 | `TestCleanup_AdoptsAlreadyMergedObject`、`TestConcurrent_CleanupVsComplete_CompleteWins` |
| A4 合并后失败留下可恢复中间态 | `TestCompleteRetry_AfterInjectedSizeFailure`、`TestCompleteRetry_AfterDatabaseStatusUpdateFailure` |
| 失败注入后再清理收敛 | `TestCleanup_ConvergesAfterInjectedCompletionFailure`、`TestCleanup_DeleteFailureThenRetryConverges` |
| A5 列表不可见待上传、并发无闪回 | `TestList_FiltersPendingShowsAdvanced`、`TestList_ConcurrentComplete_NoFlashback`、`TestList_ConcurrentAbort_PendingNeverVisible` |
| A6 重复清理幂等 | `TestCleanup_ExpiredPending_Idempotent`、`TestCleanup_AdoptsAlreadyMergedObject` 二次扫描断言 |
| A7 放弃已推进记录返回 204 且不删对象 | `TestAbort_AdvancedRecord_IdempotentNoDelete` |
| A8 三类时序 + 存储调用序列断言 | 上述并发测试；屏障由 `RecordingStore.Enable*Barrier` 与 `pg_locks` 等待器确定性调度 |
| 真实路由/中间件/HTTP 入口 | 根包 `TestRealServer_UploadFinalizationLifecycle`、`controllers.TestHTTPEntry_CompleteAbortListOverRealListener` |
| 真实 S3 适配器调用序列 | `storage.TestS3Adapter_*` |

并发测试不依赖 `time.Sleep` 猜时序：在存储调用内设置屏障卡住先到的一方，
再轮询 `pg_locks` 中 `NOT granted` 的 advisory lock，确认另一方确实在等待
同一把视频锁后释放屏障，从而确定性地构造两种胜负顺序。

### 一键运行

```bash
# 自动用 docker 启动测试 PostgreSQL（127.0.0.1:5433, dekho/dekho/dekho_test），
# 然后 go vet + go test -race -count=1 ./...
scripts/test-upload-consistency.sh

# 或手动：
docker compose -f docker-compose.test.yml up -d --wait
TEST_DATABASE_DSN='postgres://dekho:dekho@127.0.0.1:5433/dekho_test?sslmode=disable' \
  go test -race -v ./controllers ./storage .
```

测试启动时会自动按 `database/migrations/*.up.sql` 重建 schema；为防止误清真实
库，当数据库名不含 `test` 时会拒绝重置（确需对其它库运行请设
`ALLOW_NON_TEST_DB=1`）。连不上数据库时，依赖数据库的用例会 `SKIP`，
`go test ./...` 仍保持通过；`storage` 包的 S3 适配器序列测试不需要数据库。

## 5. 手工构造三类并发场景

真实服务 + 真实数据库时，可用两个终端/脚本对同一 `video_id` 同时发起请求。
为稳定复现“交叉窗口”，可在测试中使用上述屏障；手工压测则可并发重复发起：

```bash
# 先正常创建上传并 PUT 分片，得到 video_id 与 parts JSON
# 场景一：完成 与 放弃 并发
(curl -b auth.txt -X POST localhost:8080/video/$VID/complete \
   -H 'Content-Type: application/json' -d @parts.json &)
(curl -b auth.txt -X POST localhost:8080/video/$VID/abort &)
# 期望终态二选一且不交叉：status=1 + 对象在 + 列表可见；或行删除 + 分片已 abort

# 场景二：清理 与 完成 并发
# 将 upload_initiate_time 改到 25 小时前；服务启动时会立即扫一轮，
# 也可等待每 15 分钟的 tick，或在测试中同步调用 controllers.RunAbandonedUploadCleanup(db)
(curl -b auth.txt -X POST .../complete -d @parts.json &)
(重启服务 / 等待 cleanup tick &)

# 场景三：完成重试
curl -X POST .../complete -d @parts.json   # 即使返回 502/500
curl -X POST .../complete -d @parts.json   # 再试：202，且存储侧只 merge 一次
```

判定一致性的 SQL/存储核对：

```sql
SELECT video_id, status, delete_flag, source_file_key, source_upload_id
FROM videos WHERE video_id = $1;
```

再到对象存储确认 `source_file_key` 最终对象的有无，二者必须符合第 1 节矩阵。
