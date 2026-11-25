# FlashHub PR 2: Safe HTTP Range Resume

Status: **Scheme A implemented; requested automated validation passed**.
Branch: `feat/flashhub-range-resume`; stacked on PR 1 commit `441a0d6` for PR 1 review, targeting `dev` after PR 1. This is an implementation Spec, not a report of completed behavior.

## 1. Problem

PR 1 retries interrupted FlashHub downloads from byte zero. When a single download call is still active, a verified HTTP range can avoid downloading the already received prefix again.

## 2. Goals

- Resume eligible FlashHub Bundle file downloads within the existing call and three-request attempt budget.
- Keep the attempt's unique temp file private until complete-file validation and existing safe commit succeed.
- Require a strong ETag, known total, stable request/effective URL identity, identity encoding, and validated 206 metadata before appending.
- Preserve cache repair, cancellation, bounded retries, credential-safe errors, progress, and public callback contracts.
- Use Go `httptest`; test requests, transferred body bytes, final contents, and temp cleanup.

## 3. Non-goals

No cross-process/app restart recovery, persistent metadata, OS locks, checkpoints, GC, TTL/quota, multipart or parallel ranges, other downloader changes, or public API changes. A later proposal may add cross-invocation recovery after source identity stability is established; that is Future Work only.

## 4. Stacked PR and existing architecture

PR 2 is stacked on PR 1 commit `441a0d6`; PR 1 has been submitted for review. Do not rewrite PR 1 history. Integrate into `dev` after PR 1 review/merge per repository workflow.

Source facts at the PR 1 base:

| File/function | Existing behavior |
|---|---|
| `go/internal/flashhub/flashhub.go:314-403`, `DownloadFile` / `downloadFile` | Public wrapper, metadata cache, three-attempt loop, cleanup, commit recheck and rename. |
| `:432-493`, `downloadHTTPClient`; `:496-581`, `downloadAttempt` | Shared HTTP client/timeout setup; one GET per attempt, currently only 200 accepted. |
| `:583-691`, `downloadAttempt` | Unique adjacent `.flashhub-download-*`, streaming size/MD5 check, Sync/Close before promotion. Failed candidates are removed by caller. |
| `:117-158`, `acquireDownloadPath`; `:345-379`, commit section | Cancellable process-local lock covers only final validation/rename; independent transfers use independent temps. |
| `:694-709`, `matchesFileMetadata` | Existing valid cache hit; mismatch triggers automatic replacement only after new candidate validates. |
| `:799-848`, `syncFileProgress`; `SyncBundle` at `:859-904` | Live bundle progress uses per-file high-water offsets; completion follows successful download. |
| `go/internal/pythonprovision/pythonprovision.go:244-255`, `assetFromFlashHub` | Public `DownloadFile` is also used outside Bundle sync. Keep that public path non-resuming. |

Keep the transfer loop and validators. Add a private per-call resume state and opt in only from `SyncBundle`; preserve `DownloadFile` signature/semantics and Python provisioning. `DownloadManifest` also remains on the existing public path. PR 1 retry classification, timeouts, temp creation, validation, commit, and path lock are reused.

## 5. Per-call state and ownership

A private state owns one unique temp path, open/closed file lifecycle, offset, strong ETag, total, original request URL identity and effective URL identity. It is allocated by one `downloadFile` invocation, shared only among that invocation's attempts, and always cleaned on return. No state is discovered by filename or adopted from a previous call. Process crash may leave an orphan temp; do not adopt or sweep it. Do not touch legacy `<dest>.incomplete`.

Only SyncBundle file entries opt in. Public `DownloadFile`, Python provisioning and `DownloadManifest` continue full-request retries. Same-target concurrent calls keep separate candidates and PR 1 final-commit lock. Unrelated targets remain concurrent. Conflicting index metadata keeps the existing last-successful-commit limitation; each candidate must pass its own metadata checks.

## 6. HTTP state machine

Initial GET has no Range. If it returns 200, establish resume eligibility only if total is known (positive indexed `File.Size` or response Content-Length; when both exist they must agree), ETag is syntactically strong, body encoding is absent/identity, and actual effective URL equals the requested URL. Otherwise stream as a normal full candidate and restart from zero after interruption.

For an eligible interrupted response with offset `N > 0`, send `Range: bytes=N-` and `If-Range: <strong ETag>`, with `Accept-Encoding: identity`. URL source identity includes Userinfo as well as scheme, host, escaped path and query; fragments do not identify an HTTP resource because they are not sent to the server. Do not send old If-Range if source identity is uncertain. For ranged requests, do not follow redirects: treat 3xx as a reason to discard the prefix and spend a remaining attempt on an ordinary full request. For an initial full request, retain configured redirect behavior; a redirected response is ineligible for future range reuse when any compared identity component changes.

| Response | Required behavior |
|---|---|
| 200 to full GET | Start at zero; validate known lengths and full body. |
| 206 to Range | Validate all rules below before append. |
| 200 to Range | Server ignored Range or validator changed; truncate only this call's temp and consume this response from byte zero. Refresh/disable eligibility from this full response. |
| 400/405/501 to Range | Discard the prefix and spend one remaining attempt on an un-ranged full GET; do not retry 401/403/404. |
| 416 to Range | Accept only if Content-Range is exactly `bytes */T`, local offset equals T, validator and total match, and the whole candidate passes Size/MD5. Otherwise discard prefix and use a remaining full attempt. |
| malformed/mismatched 206, changed ETag/source/encoding, unsupported range | Never append; discard prefix and spend one remaining attempt on full GET. Repeated failures terminate at the existing three-request cap. |
| 408/429/5xx or retryable transport/body failure | Preserve the eligible in-call prefix; retry within the original budget. Honor bounded Retry-After for 429. |
| ordinary permanent 4xx | Return redacted status error without unconditional retry. |
| cancellation/timeout | Stop promptly; keep the candidate only within the active call, then clean it before return. |

Every full or range GET, including fallback, consumes exactly one attempt. No secondary retry loop and at most three application requests. Backoff and all existing connect/header/no-progress/attempt timeouts remain in force.

## 7. Range and body validation

Accept only one `Content-Range` field matching `bytes N-E/T`, decimal nonnegative int64 values, `0 <= N <= E < T`, known T, `N == local offset`, and `E == T-1` for the open-ended Range request. Reject duplicate fields, comma/multipart responses, unknown totals, overflow, trailing text, and partial segments that require another chunk request.

Require response strong ETag to equal the original strong ETag exactly. Require `T` to equal the pinned total and positive `File.Size` when supplied. `Content-Length`, when known, must equal `E-N+1`. Count body bytes and require exact length at EOF; reject excess bytes. Go transport truncation is retryable only when it is a transport interruption and the retained prefix remains eligible. Size/MD5 mismatch is terminal and never retried as an integrity success. Whole-file MD5 re-reading and 416 candidate validation use the current attempt context, so their time counts against the existing attempt deadline. Parent cancellation is terminal; attempt timeout is retryable only within the original request budget. No candidate may be committed after either context expires.

Require `Accept-Encoding: identity`; reject non-identity Content-Encoding and `response.Uncompressed`. A range request is not automatically followed through redirects: this prevents leaking an old If-Range validator to a changed host/resource. The redirected full fallback may follow the configured client policy, but the final effective URL mismatch disables subsequent Range. Compare URL identity in memory; never persist/log signed URL, userinfo or query. Errors continue to redact raw URLs.

If the response total or strong ETag is absent, weak, malformed, or inconsistent, do not resume. `Last-Modified` is diagnostic only; it does not authorize Range. If Size, MD5, ETag and Last-Modified are all absent, do full-download retries only.

## 8. Integrity, cache and commit

Retain PR 1's existing cache and repair policy: valid target under available Size/MD5 hits cache; invalid target is automatically repaired by downloading a new candidate. Preserve old target bytes until a fully validated new file is ready. For resumed completion, verify the entire candidate against positive `File.Size`, nonempty MD5, and pinned response total before Sync/Close and commit. Never hash only the suffix or publish a partial candidate.

Do not add completion receipts or conflict blocking for legacy targets. Same target with conflicting metadata across concurrent independent calls remains last-successful-commit as in PR 1; explicitly document this limit. Old `.incomplete` files remain untouched. Windows/Linux rename behavior must preserve existing destination on failure and be exercised by platform tests.

## 9. Progress and public API

Do not change public `DownloadFile` or `onBytes`: callback bytes are emitted only after successful commit. SyncBundle's private live callback receives cumulative logical offset `N + newly written bytes`, never suffix bytes alone. Keep high-water accounting across retries, clamp known-size progress below completion until the entire file validates, and do not report completion on fallback or failed validation. Unknown-size files do not consume a known aggregate total. Do not change progress schema.

## 10. Acceptance criteria

- Eligible single-call interruptions resume with correct Range/If-Range and demonstrably fewer response-body bytes.
- No invalid/mismatched response is appended; ignored Range safely restarts at zero.
- The original maximum of three requests applies to full, range and fallback requests together.
- Full Size/MD5/response-total verification precedes commit; existing damaged cache repair and valid cache hit remain.
- Context/timeout behavior, credential redaction, public API and Python provisioning remain unchanged.
- SyncBundle reports bounded live cumulative progress without duplicate counting.
- Owned temps are closed and removed on every return path.
- Windows and Linux focused tests pass; Linux race and full Go suite pass as requested for implementation.

## 11. Test matrix

All network tests use tiny Go `httptest` payloads. Range tests assert the relevant request headers, server-counted response bytes, final file contents/status and temp cleanup; cases that exercise replacement explicitly assert the old destination state.

The package-private `Client.hashFile` hook is used only to deterministically block or cancel the validation stage in tests. It is nil in normal clients, which use the context-aware `fileMD5Context` implementation.

| Test case | Expected request and assertions |
|---|---|
| Normal complete download / valid cache / damaged cache | One GET without Range; correct cache hit or repaired final; no temp remains. |
| Interrupted eligible body then valid 206 | First GET no Range; second has exact `Range` and `If-Range`; server confirms only suffix bytes sent; exact final file. |
| Range ignored with 200 | Second request has Range; server returns full body; implementation truncates own temp and sends no extra fallback GET. |
| Range explicitly rejected with 400/405/501 | Discard the prefix and use one remaining request for an un-ranged full GET; auth/permission/not-found errors remain permanent. |
| Invalid Content-Range/duplicate/multipart/wrong start/end/total | No response bytes appended; bounded full fallback; exact final or bounded error. |
| 416 exact/incorrect offset or validator | Verify only exact complete candidate may pass; otherwise bounded full fallback; existing target preserved. |
| Changed, weak or missing ETag; unknown total; changed source/redirect/Userinfo | No old prefix appended; Range is disabled or fallback is used; assert host, credentials identity and headers observed by handler. |
| Content-Range parser boundaries and offset/body lengths | Table-test duplicate/wrong-unit/unknown/negative/overflow/reversed/out-of-total values; integration-test offset mismatch and short/long body without invalid commit. |
| Content-Length mismatch, short/long body, compressed response | No invalid final commit; exact bounded attempts and cleanup. |
| Multiple interruptions, third attempt succeeds; all attempts fail | Exact request count <=3, offsets/body counts correct, no temp after return. |
| Context cancellation, transport timeout, retryable 429/5xx | Timely cancellation; no extra request; retry budget/backoff retained. |
| Size/MD5 mismatch | Terminal error; existing destination unchanged; no temp. |
| Hash validation timeout/cancellation; temp reopen failure | No unverified candidate is published; old target remains; only the call-owned temp is cleaned. |
| Concurrent same target and conflicting metadata | Independent candidates, serialized existing commit behavior, final data passes its caller's validators; document last-writer limit. |
| Live SyncBundle progress during Range retry | Cumulative offsets, no double count or >100%, completion only after validation. |
| Public DownloadFile/Python provision regression | No Range header; original callback and full-download retry semantics. |
| PR 1 regression suite | All prior `internal/flashhub` tests continue to pass. |

No process restart, persisted record, checkpoint, lock or GC tests are required for this scope.

## 12. Files, implementation order and risks

Actual changes: `go/internal/flashhub/flashhub.go`, new `go/internal/flashhub/resume_test.go`, this Spec, and synchronized `flashhub-range-resume.zh-CN.md`. The PR 1 `download_test.go` suite remains unchanged. No go.mod/go.sum, CLI, server, host, weights, installer, Desktop or pythonprovision changes.

Completed in order: (1) rewrote this Spec and its Chinese mirror; (2) added httptest coverage for request headers, server-sent bytes and final state; (3) added private per-call state while retaining the existing retry loop; (4) implemented response validation and bounded fallback; (5) reused full-file verification and the existing commit path; (6) checked progress, cache repair, concurrency and public API behavior; (7) ran the requested Windows and WSL/Linux gates and reviewed the diff.

Source support for Range/If-Range was measured on two public CDN objects in the second design review, but redirects and signed URL rotation were not observed. httptest validates client decisions, not every CDN edge case. PR 1 temp cleanup can leave an orphan after process crash; A deliberately does not recover it. The requested Windows focused test, WSL/Linux focused test, race test and full Go suite passed. The Windows full Go suite was not run; the known unrelated Windows weights-path assertion remains outside this PR's scope.

Relationship to PR 1: this is a stacked PR based on `441a0d6`; retain all PR 1 guarantees and history. PR 1 authoritative specs remain separate; do not silently edit them.

## 13. Validation record

| Environment | Command | Result |
|---|---|---|
| Windows Go | `go test ./internal/flashhub -count=1 -v` | Passed. |
| WSL `flashserver_ubuntu`, Go 1.24.13 | `/usr/local/go/bin/go test ./internal/flashhub -count=1` | Passed. |
| WSL `flashserver_ubuntu`, Go 1.24.13 | `/usr/local/go/bin/go test -race ./internal/flashhub -count=1` | Passed. |
| WSL `flashserver_ubuntu`, Go 1.24.13 | `/usr/local/go/bin/go test ./... -count=1 -timeout 900s` | Passed. |

The existing FlashServer process was left running. The separate Ubuntu 24.04 WSL launch was unavailable due to its WSL networking/proxy setup, so the already-running `flashserver_ubuntu` distribution was used. No Windows full-suite result is claimed.
