# 部署与运维

## 健康检查

- API 存活：`GET /health/live`
- API 就绪：`GET /health/ready`
- PostgreSQL：`pg_isready`
- Redis：`redis-cli ping`
- MinIO：`mc ready local`
- ClamAV：`clamdcheck.sh`

## 关键监控

- API 错误率、p50/p95/p99 延迟。
- PostgreSQL 连接数、慢查询和磁盘使用率。
- Redis 内存、BullMQ 等待任务和失败任务。
- `media_assets` 中 `processing` 或 `failed` 数量。
- `manual_review` 媒体队列长度。
- `pending` 内容与评论队列长度。
- outbox `pending`、`failed` 数量。
- outbox `processing` 且租约已过期的数量（应快速回落为 0）。
- outbox `ambiguous_count > 0` 的事件数量（每次增长都应对应一次可解释的投递中断）。
- `delete_after <= now()` 的原图数量。
- 公开桶中是否存在未被数据库引用的对象。

## 邮件投递幂等边界

outbox 投递遵循两条边界，任何重放都必须满足：

- **跨实例认领边界**：事件只有被某个 worker 实例持租认领（`claimed_by` + `claim_token` + `lease_expires_at`）后才能投递；认领是单条原子 SQL（`FOR UPDATE SKIP LOCKED`），认领域为「`pending` 且到点」或「`processing` 且租约过期」。之后每次状态写回都按 `claim_token` 围栏校验，租约失效的旧实例无法确认或失败事件。租约（`OUTBOX_LEASE_SECONDS`，默认 120s）必须大于单次 SMTP 超时（`OUTBOX_SMTP_TIMEOUT_MS`，默认 30s），worker 启动时强制校验。
- **历史重放边界**：只有「确定未送达」（SMTP 明确拒绝、连接未建立）的事件可自由重放，受 `OUTBOX_MAX_ATTEMPTS`（默认 5）限制；「结果不明」（连接建立后断线/超时、worker 崩溃）的事件以同一身份重放——`delivery_key` 渲染为确定性 `Message-ID` 与 `X-Outbox-Delivery-Key` 头——且不明次数受 `OUTBOX_MAX_AMBIGUOUS`（默认 2）限制，超限即置 `failed` 停驻，等待人工对账，不再自动重放。

每次物理投递都先落账到 `outbox_delivery_attempts`（含 `message_id`），成功确认只追加事实、不改写原始 `payload`，因此任何事件的投递历史都可从数据库重放推导。对账时用 SMTP 日志中的 Message-ID 关联：

```sql
SELECT e.id, e.status, a.attempt_no, a.status, a.error
FROM outbox_delivery_attempts a
JOIN outbox_events e ON e.id = a.event_id
WHERE a.message_id = '<delivery_key@域名>';
```

人工确认邮件确实未送达后，可将停驻事件重新置回队列：

```sql
UPDATE outbox_events
SET status = 'pending', available_at = now(), ambiguous_count = 0, updated_at = now()
WHERE id = '<event-id>' AND status = 'failed';
```

## 备份

- PostgreSQL 每日全量备份并保留 WAL 或等价连续归档。
- MinIO 启用版本化和跨盘/跨区域容灾时，分别备份隔离桶和公开桶。
- `.env.production` 和密钥应保存在密钥管理系统，不进入镜像或仓库。
- 每季度执行一次恢复演练，验证数据库、公开媒体和迁移记录。

## 发布

1. 构建并锁定 API、Worker、Web 镜像。
2. 备份数据库。
3. 执行一次 `migrate` 容器。
4. 启动新 API 和 Worker。
5. 验证就绪检查、登录、地图查询和媒体处理。
6. 再切换 Caddy 流量。
7. 保留上一版本镜像用于回滚。

## 隐私事件

发现未模糊媒体或原图泄露时：

1. 立即停止相关媒体发布并删除公开对象。
2. 暂停媒体 Worker，防止继续复制到公开桶。
3. 根据对象访问日志确认影响范围。
4. 修复处理管线并执行全量扫描。
5. 删除或隔离受影响对象。
6. 记录事故、根因、修复和回归测试。
7. 按法律与运营要求通知用户。

## 数据保留

- 成功处理原图：24 小时。
- 失败处理原图：最多 7 天。
- 邮箱验证令牌：24 小时。
- 密码重置令牌：30 分钟。
- 过期刷新令牌：30 天清理。
- 账号删除冷静期：30 天。
- 审计与审核记录：默认 180 天，生产可按法务要求延长。
