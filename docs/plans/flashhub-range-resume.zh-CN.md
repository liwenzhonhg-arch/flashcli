# FlashHub PR 2：安全 HTTP Range 续传

状态：**方案 A 已实现；指定自动化验证通过**。
分支：`feat/flashhub-range-resume`；基于 PR 1 Commit `441a0d6` 的 Stacked PR，PR 1 审核后按仓库流程合入 `dev`。

## 1. 问题

PR 1 遇到中断会从字节 0 重试。若同一次下载调用仍在运行，可用经过校验的 HTTP Range 避免重复传输已收到的前缀。

## 2. 目标

- 在原有调用和每文件三次请求预算内续传符合条件的 FlashHub Bundle 文件。
- 使用本次调用独占的随机临时文件；完成校验并安全提交之前不进入正式缓存。
- 仅当强 ETag、已知总长、请求 URL 与实际来源一致且使用 identity 编码时，才尝试续传；206 必须逐项验证后才追加。
- 保留缓存修复、取消、有界重试、错误脱敏、进度及公开回调契约。
- 使用 Go `httptest` 检查请求、服务端实际发送字节数、最终文件和临时文件清理。

## 3. 非目标

不实现跨进程/应用重启恢复、持久元数据、OS 锁、Checkpoint、GC、TTL/配额、多线程分片、其他下载器重构或公开 API 变化。未来可在远端资源身份稳定性得到验证后另行设计跨调用恢复；本 PR 不包含该功能。

## 4. Stacked PR 与现有架构

PR 2 基于 PR 1 Commit `441a0d6`；PR 1 已提交审核。本次不重写 PR 1 历史，待 PR 1 审核/合并后按仓库流程集成到 `dev`。

源码事实（以该基准提交为准）：

| 文件/函数 | 现有行为 |
|---|---|
| `go/internal/flashhub/flashhub.go:314-403`，`DownloadFile` / `downloadFile` | 公开包装、缓存校验、三次尝试循环、失败清理、提交前复核和重命名。 |
| `:432-493`，`downloadHTTPClient`；`:496-581`，`downloadAttempt` | 共用 HTTP 客户端/超时；每次仅发一个 GET，目前只接受 200。 |
| `:583-691`，`downloadAttempt` | 创建目标目录旁唯一 `.flashhub-download-*`，流式校验 Size/MD5，Sync/Close 后才提交；失败候选由调用者清理。 |
| `:117-158`，`acquireDownloadPath`；`:345-379`，提交部分 | 可取消的进程内锁只保护最终校验和重命名；下载期间各自使用临时文件。 |
| `:694-709`，`matchesFileMetadata` | 当前元数据验证通过则命中缓存；不匹配时，只有新候选通过验证才自动替换。 |
| `:799-848`，`syncFileProgress`；`SyncBundle` 的 `:859-904` | Bundle 实时进度按每文件最高逻辑偏移累计；成功下载后才完成计数。 |
| `go/internal/pythonprovision/pythonprovision.go:244-255`，`assetFromFlashHub` | 公开 `DownloadFile` 还用于 Bundle 之外的 Python 供应。保持该路径不续传。 |

保留现有下载循环和校验器，增加私有的单次调用续传状态，仅由 `SyncBundle` 启用。公开 `DownloadFile` 的签名/行为和 Python 供应不变。`DownloadManifest` 也继续走现有公开路径。复用 PR 1 的重试分类、超时、临时文件、校验、提交和路径锁。

## 5. 单次调用状态与所有权

私有状态保存本次调用的临时路径、文件生命周期、偏移、强 ETag、总长、原始请求 URL 身份和实际响应 URL 身份。它只由一个 `downloadFile` 调用创建并供本次尝试使用，函数返回前必须清理。不能按文件名发现或接管旧文件。进程崩溃可能留下随机孤立临时文件；不得自动接管或扫描删除。旧版 `<dest>.incomplete` 保持不动。

仅 `SyncBundle` 文件项启用此能力。公开 `DownloadFile`、Python 供应和 `DownloadManifest` 仍从头重试。同目标并发调用继续使用独立候选和 PR 1 提交锁；无关目标可以并发。不同元数据的并发仍有 PR 1 的“最后成功提交者胜出”限制，必须明确记录，每个候选仍需通过自己的完整性校验。

## 6. HTTP 状态机

初始 GET 不带 Range。若返回 200，只有在总长已知（正数索引 `File.Size` 或响应 Content-Length；若两者都存在必须相等）、ETag 是语法有效的强标签、编码为空/identity、实际响应 URL 与请求 URL 相同时，才允许后续 Range。否则仍正常下载完整候选，但中断后的重试从头开始。

符合条件且偏移 `N > 0` 时发 `Range: bytes=N-` 和 `If-Range: <strong ETag>`，并请求 `Accept-Encoding: identity`。URL 来源身份包括 Userinfo、scheme、host、转义路径和 query；fragment 不会发送给服务器，因此不标识 HTTP 资源。来源不确定时绝不发送旧 If-Range。Range 请求不跟随重定向：遇到 3xx 就丢弃前缀，用剩余一次尝试发普通完整 GET。初始完整请求保留原配置的重定向策略；比较的身份组成部分发生变化时，禁止后续 Range。

| 响应 | 必须行为 |
|---|---|
| 完整 GET 返回 200 | 从 0 写入；校验已知总长和完整响应体。 |
| Range 返回 206 | 满足下节所有验证后才追加。 |
| Range 返回 200 | 服务端忽略 Range 或版本变化；只截断本次调用自己的临时文件，从头消费当前响应，并按新完整响应更新或禁用续传条件。 |
| Range 返回 400/405/501 | 丢弃前缀，在剩余预算内用一次无 Range 的完整 GET 回退；401/403/404 不重试。 |
| Range 返回 416 | 仅当 Content-Range 严格为 `bytes */T`、本地偏移等于 T、验证器/总长相同且完整文件通过 Size/MD5 时接受。否则丢弃前缀并在剩余预算内完整重下。 |
| 错误/不匹配 206、ETag/来源/编码变化、不支持 Range | 绝不拼接；丢弃前缀，并消耗一次剩余尝试进行完整 GET。重复故障最多三次后终止。 |
| 408/429/5xx 或可重试的网络/响应体中断 | 保留本次调用中仍有效的前缀；在原预算内重试。429 使用原有有界 Retry-After。 |
| 普通永久 4xx | 返回脱敏状态错误，不无条件重试。 |
| 取消/超时 | 尽快停止；在返回前清理本次候选。 |

每次完整 GET、Range GET 和回退请求都消耗一次尝试。没有第二重试循环，单文件请求总数最多三次。保留现有退避、连接/响应头/无进展/总时长限制。

## 7. Range 与响应体校验

只接受单个 `Content-Range`，格式为 `bytes N-E/T`，十进制非负 int64，`0 <= N <= E < T`，T 已知、N 等于本地偏移、开放式 Range 的 E 必须为 `T-1`。拒绝重复字段、逗号/多段响应、未知总长、溢出、尾随字符及需要继续分段请求的短范围。

响应强 ETag 必须与最初的强 ETag 完全相等。T 必须与固定总长一致，若索引提供正数 `File.Size` 也必须相同。存在 Content-Length 时必须等于 `E-N+1`；实际读取字节也必须精确相同，拒绝多余字节。Go transport 检出的短响应仅在其属于可重试传输中断且保留前缀仍符合续传条件时重试。Size/MD5 不匹配是完整性失败，不能当作成功重试。整文件 MD5 重读和 416 候选核验都使用本次 attempt 的 context，验证耗时计入既有超时预算。调用方取消为终止错误；attempt 超时只能在原请求预算内重试。任一 context 过期后都不得提交候选。

请求 `Accept-Encoding: identity`；拒绝非 identity 的 Content-Encoding 或 `response.Uncompressed`。Range 请求不自动跟随重定向，避免把旧 If-Range 发往新主机/资源。完整回退可遵循已有重定向策略，但最终 URL 与原请求不同就禁用后续 Range。只在内存比较 URL 身份，不保存或记录带签名 query、userinfo 的原始 URL；错误继续脱敏。

缺失/弱/语法错误 ETag、未知或冲突总长均禁止续传。Last-Modified 只作诊断，不能授权 Range。Size、MD5、ETag 和 Last-Modified 全缺失时只能完整下载重试。

## 8. 完整性、缓存与提交

保留 PR 1 缓存和修复策略：已有目标通过可用 Size/MD5 时直接命中；不匹配则自动下载候选。新文件成功完整校验前，旧目标保持不变。续传完成后必须对整个候选检查正数 `File.Size`、非空 MD5 和固定响应总长，然后 Sync/Close 并提交；不能只校验后缀或发布部分文件。

不添加完成收据，也不阻断无记录的旧缓存。同一目标不同元数据的独立并发下载仍可能由最后成功提交者决定内容，这是 PR 1 已有的限制。旧 `.incomplete` 文件不自动触碰。Windows/Linux 的重命名必须在失败时保护旧目标，并在对应平台测试。

## 9. 进度与公开 API

不改公开 `DownloadFile` 或 onBytes：只在成功提交后回报字节。SyncBundle 内部回调报告累计逻辑偏移 `N + 本次新写入字节`，不能只报后缀。保留 high-water 防止重试重复累计；已知 Size 的进度在整文件校验前不得完成；回退或校验失败不应标记完成。未知大小文件不占用已知聚合总量。不改进度 schema。

## 10. 验收标准

- 符合条件的单次调用中断续传使用正确 Range/If-Range，且可从服务器发送字节数证明少传了前缀。
- 错误/不一致响应不会被拼接；忽略 Range 时从 0 安全重下。
- 完整、Range、回退请求合计使用原有最多三次预算。
- 完整 Size/MD5/响应总长校验完成后才提交；原有损坏缓存自动修复和有效缓存命中保留。
- Context/超时、错误脱敏、公开 API 和 Python 供应行为不变。
- SyncBundle 实时进度按累计偏移报告，不重复计数、不超过 100%。
- 所有返回路径都关闭并清理本次临时文件。
- Windows 和 Linux 专项测试通过；Linux race 与按任务要求的 Go 全量测试通过。

## 11. 测试矩阵

网络测试均使用小型 Go `httptest` 内容。续传测试检查相关请求头、服务端实际发送字节数、最终文件状态及临时文件清理；涉及替换的用例会明确检查旧目标状态。

包内私有的 `Client.hashFile` 钩子仅用于在测试中稳定阻塞或取消校验阶段。正常客户端不设置该字段，仍使用支持 Context 的 `fileMD5Context`。

| 场景 | 请求与断言 |
|---|---|
| 正常完整下载、有效缓存、损坏缓存 | 一次无 Range GET；缓存命中或成功修复；最终文件正确且无临时文件。 |
| 可续传中断后合法 206 | 首次无 Range，第二次带精确 Range/If-Range；服务器确认仅发送后缀字节；最终内容逐字节一致。 |
| 忽略 Range 返回 200 | 第二次请求带 Range；服务器发送完整体；程序从 0 覆写自己的临时文件，不额外再发回退 GET。 |
| 服务端以 400/405/501 明确拒绝 Range | 丢弃前缀，在剩余预算内发送一次无 Range 的完整 GET；认证、权限和资源不存在等错误仍视为永久错误。 |
| 错误/重复/multipart Content-Range、起点/终点/总长错误 | 不拼接响应字节；有界完整回退或错误；最终文件精确正确。 |
| 416：精确/不符偏移或验证器 | 仅完整候选及所有验证条件通过才成功，否则有界回退；旧目标不变。 |
| ETag 改变/弱/缺失、未知总长、来源或重定向改变 | 不拼旧前缀；禁用 Range 或安全回退；测试 handler 检查目标主机和头。 |
| Userinfo 变化及 Content-Range 边界 | 表驱动检查重复头、单位、未知/负总长、溢出、逆序和越界；集成检查 URL 凭据来源、偏移不符、短体/长体均不错误提交。 |
| Content-Length 错、短体/长体、压缩响应 | 不提交错误文件；请求次数有界且清理正确。 |
| 多次中断、第三次成功、三次都失败 | 请求总数不超 3；偏移/传输量正确；返回后无临时文件。 |
| Context 取消、传输超时、429/5xx | 快速取消；不多发请求；复用现有退避与预算。 |
| Size/MD5 不符 | 明确失败；旧目标不变；无临时文件。 |
| 整文件核验超时/取消、临时文件重新打开失败 | 不发布未验证候选；旧目标保持；只清理本次调用拥有的临时文件。 |
| 同目标并发与冲突元数据 | 候选互相独立；最终提交仍符合各调用自身验证规则；记录 last-writer 限制。 |
| SyncBundle Range 实时进度 | 累计偏移、不重复、不超过 100%，验证成功后才完成。 |
| 公开 DownloadFile / Python 供应回归 | 不发送 Range；保持原回调及完整下载重试语义。 |
| PR 1 测试集 | `internal/flashhub` 原有测试全部通过。 |

不要求进程重启、持久记录、Checkpoint、OS 锁或 GC 专属测试。

## 12. 文件、实施顺序与风险

实际改动：`go/internal/flashhub/flashhub.go`、新增 `go/internal/flashhub/resume_test.go`、本文档及同步的 `flashhub-range-resume.zh-CN.md`。PR 1 的 `download_test.go` 测试集保持未修改。不修改 go.mod/go.sum、CLI、server、host、weights、installer、Desktop 或 pythonprovision。

已按以下顺序完成：(1) 整理 Spec 和中文镜像；(2) 添加检查请求头、服务端发送字节数和最终状态的 httptest；(3) 加入单次调用私有状态并保留原重试循环；(4) 实现响应校验和有界回退；(5) 复用整文件验证与现有提交路径；(6) 检查进度、缓存修复、并发和公开 API 行为；(7) 执行 Windows 与 WSL/Linux 指定验证并审查 diff。

第二轮设计审查已在两个公开 CDN 资源上实测 Range/If-Range，但未观察重定向或签名 URL 轮换。httptest 能证明客户端逻辑，不能证明所有 CDN 边缘行为。PR 1 临时文件在进程崩溃时可能成为孤儿；方案 A 不恢复它。本轮 Windows 定向测试、WSL/Linux 定向测试、race 测试和 Go 全量测试均通过。未运行 Windows 全量测试；已知无关的 Windows weights 路径断言仍在本 PR 范围之外。

与 PR 1 的关系：这是基于 `441a0d6` 的 Stacked PR；保留 PR 1 所有保证和历史。PR 1 权威 Spec 保持独立，不在此处修改。

## 13. 验证记录

| 环境 | 命令 | 结果 |
|---|---|---|
| Windows Go | `go test ./internal/flashhub -count=1 -v` | 通过。 |
| WSL `flashserver_ubuntu`，Go 1.24.13 | `/usr/local/go/bin/go test ./internal/flashhub -count=1` | 通过。 |
| WSL `flashserver_ubuntu`，Go 1.24.13 | `/usr/local/go/bin/go test -race ./internal/flashhub -count=1` | 通过。 |
| WSL `flashserver_ubuntu`，Go 1.24.13 | `/usr/local/go/bin/go test ./... -count=1 -timeout 900s` | 通过。 |

保留运行中的 FlashServer 进程，未停止或干扰。另一个 Ubuntu 24.04 WSL 因自身 WSL 网络代理配置无法启动，因此使用已有的 `flashserver_ubuntu` 发行版执行 Linux 验证。未宣称 Windows 全量测试结果。
