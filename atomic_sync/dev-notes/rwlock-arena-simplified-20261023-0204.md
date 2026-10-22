# cooperative_arena：RootRead 简化设计（feat/simp-read）

分支：`feat/simp-read`
基线：`4d0006c`（`feat/rwlock-arena` 的 WaitNode/WakeNode 版本）

## 1. 目标

在不改变对外 API、不改变共享行为测试语义的前提下，简化 `RootRead` 的
并发记账与唤醒协议。原设计为了把读者领取/释放完全移出队列锁，引入了：

- `count_` 与 `pending_` 两个原子；
- 独立的 `upgr_active_` 原子；
- `release_ops_` 并发收尾计数；
- `RootState.upgrade_` 升级预留节点 + `SlotState::Reserved`；
- `sealed_` / `waking_` / `linked_` 标记与 `retry_` 重试；
- 每个 `ReaderSlot` / `WriteSlot` 一把自旋锁。

本次按以下方式收敛：

1. **单一持锁计数**：不再区分“持锁者”和“已放行未领取”。读者被放行时
   `count += 1`；释放、或已放行后取消，都直接 `count -= 1`。取消的节点标
   `Cancelled`，唤醒遍历会跳过它。
2. **可升级读标记并入同一个原子**：`RootRead.state_` 的低位是计数，最高位
   是 `UPGR` 标记；不再有独立的 `upgr_active_` 原子。
3. **删除 `release_ops_`**：计数减到 0 的线程唯一；它只触发一次
   `run_pass_`，由队首 prune 回收空组，不在释放路径里再访问组指针。
4. **删除升级预留**：不再有 `RootState.upgrade_`、`Reserved` 状态或休眠
   写节点。升级请求真正发起时才插入 `PureWrite(Upgrade)`：来自队列放行的
   可升级读插到对应 `RootRead` 后继，快速路径可升级读插到队首。
5. **删除 `sealed_` / `waking_` / `linked_`**：
   - 能否加入读者链由“该组是否主队列尾”决定；写者或升级节点一旦排到组后，
     组就不再是尾，新读者自然创建新组。`UpgrRead` 的唯一性由扫描读者链
     决定，普通 `Read` 可以排在 `UpgrRead` 之后。
   - `waking_` 由队列锁本身替代：选择、批量放行、取 waker 都在锁内完成；
     锁外只调用 `Waker::wake`。谁抢不到锁就自旋等待，不需要 `retry_`。
   - `RootRead` 在被访问时一定仍在主队列中；写者节点由领取/取消方唯一地
     摘链释放，因此不需要 `linked_`。
6. **删除节点级自旋锁**：`ReaderNode` / `WriteNode` 的 `state` 与 `waker`
   都由队列锁保护，节点本身不再加锁。

## 2. 数据结构

```text
WaitNode { prev_, next_, content_: WakeNode }

WakeNode::RootRead(RootRead)
WakeNode::PureRead(ReaderNode)
WakeNode::UpgrRead(ReaderNode)
WakeNode::PureWrite(WriteNode)

RootRead {
    state_: AtomicUsize,              // count（低位） + UPGR（最高位）
    tail_:  AtomicPtr<WaitNode>,      // 最后加入的读者；CAS 压栈
}

ReaderNode {
    root_:  NonNull<WaitNode>,
    prev_:  *mut WaitNode,                    // 读者链 LIFO 前驱（随节点一起发布）
    state_: AtomicU8,                         // Registering/Waiting/Taking/...
    waker_: UnsafeCell<Option<Waker>>,        // 只在 Registering/Taking 独占访问
}

WriteNode {
    kind_:   WaitKind,                        // Write / Upgrade
    root_:   Option<NonNull<WaitNode>>,       // 升级节点所属组
    counted_: Cell<bool>,                     // 计入 writer_waiters_
    state_:  Cell<SlotState>,                 // 队列锁
    waker_:  UnsafeCell<Option<Waker>>,       // 队列锁
}
```

`SlotState` 只有 `Waiting` / `Admitted` / `Acquired` / `Cancelled`；
`Reserved` 已删除。

### 2.1 `RootRead.state_`

```text
bit N-1        UPGR     当前是否有已领取的可升级读持锁者
bit [0, N-2]   COUNT    已放行且尚未释放的读者数（含未领取）
```

- `inc_count_()`：放行一个读者时 `fetch_add(1)`；
- `dec_count_()`：释放/取消时 `fetch_sub(1)`，返回减一前的计数；
- `set_upgr_active_()` / `clear_upgr_active_()`：`fetch_or` / `fetch_and` 最高位；
- 最后一个 `dec_count_() == 1` 的线程触发回收；可升级读释放时先清 `UPGR`
  再减计数。

## 3. 队列锁协议

### 3.1 入队

- `Read` / `UpgradableRead`：
  - 若主队列尾是 `RootRead`，且（`Read` 或链中还没有 `UpgrRead`）则把新
    节点原子地压入该组的读者链（`RootRead.tail_` CAS，`prev_` 指向旧尾）；
  - 否则新建 `RootRead` 并追加到主队列尾；
  - 节点分配仍在队列锁内（Arena 由队列锁保护），但链入本身是 CAS，
    注册/领取/取消完全在锁外。
- `Write`：新建 `PureWrite(Write)` 追加到主队列尾。
- `Upgrade`：
  - 有活跃读者组（`tracked`）时，新建 `PureWrite(Upgrade)` 插到该组后继；
  - 无组（快速路径可升级读）时，插到主队列队首。
- 入队、`writer_waiters_` / `WRITER_QUEUED` 维护、waker 注册都在同一段队列
  锁临界区内完成。

### 3.2 放行与唤醒

`run_pass_()` 每轮：

1. 取队列锁，`prune_front_` 回收队首空组；
2. `select_head_` 选出：
   - 有等待读者的 `RootRead`；
   - 或可放行的 `PureWrite`（普通写者 / 升级写者）；
3. 在锁内：
   - 沿 `RootRead.tail_` 的 `prev_` 链最多处理 `WAKE_BATCH = 32` 个读者；
     对 `Waiting` 节点用 CAS `Waiting -> Taking` 取走 waker，再标 `Admitted`，
     `count += 1`；`Registering` / `Taking` 节点由各自的所有者推进，遍历跳过；
   - 对写者：取 waker、标 `Admitted`；
4. 释放锁，`WakerBatch::wake_all()`；
5. 回到 1，重新从队首选择，继续下一批。

因此：

- 队列锁内绝不调用 `Waker::wake`；
- 不需要 `waking_` 标记：选择/放行/取 waker 全部由队列锁串行化；
- 不需要 `retry_`：重新挂起的读者会在自己的 `run_pass_` 里重新被选中；
- 每一批最多 32 个 waker，超过时下一轮继续，不分配堆内存。

### 3.3 升级写者的放行

- 组后继存在 `PureWrite(Upgrade)` 且
  `RootRead.count_ == 1 && RootRead.upgr_active_()`、
  `READER_COUNT == 1 && UPGRADE_ACTIVE` 时，`select_head_` 直接选中该节点；
- 节点处于 `Waiting` 时取 waker 并标 `Admitted`，锁外唤醒；
- 升级 future 被唤醒后在自己的 `poll` 里尝试 `try_upgrade_to_write`，成功后
  节点由 `finish_write_node_` 摘链释放。

## 4. 取消与收尾

### 4.1 取消

- `Waiting` 读者：标 `Cancelled`、清 waker；不在计数里，无需减。
- `Admitted` 读者：标 `Cancelled`、清 waker、`count -= 1`；后续放行跳过它。
- `Waiting/Admitted` 写者：标 `Cancelled`、清 waker；若已计入
  `writer_waiters_` 则减一，随后摘链释放节点。

### 4.2 最后一个读者

```text
release_group_reader_(is_upgradable):
    if is_upgradable: clear UPGR
    old = dec_count()
    if old == 1:
        run_pass_()        // 队首 prune 回收空组
```

- 释放路径**不再使用组指针**做任何收尾，因此不存在“2→1 的线程还在访问组、
  1→0 的线程已经回收”的竞争，不需要 `release_ops_`。
- 空组只由 `prune_front_` 在队列锁内回收；如果空组不在队首，它会在到达队首
  时被回收，不会泄漏。
- `RootRead.count_ == 0 && !has_waiting_()` 是回收条件；`UPGR` 必须已清除
  （`debug_assert` 校验）。

## 5. 与旧设计的差异

| 项目 | 旧设计 | 简化设计 |
|---|---|---|
| 读者计数 | `count_` + `pending_` 两个原子 | 一个 `state_` 原子（count + UPGR） |
| 可升级读标记 | 独立 `AtomicBool` | 并入 `state_` 最高位 |
| 释放收尾 | `release_ops_` 防并发回收 | 最后一个计数触发 `run_pass_`，无 release_ops |
| 升级写节点 | 计数减到 1 时预留 `Reserved` 节点 | 升级请求真正发起时才插入 |
| 读者链封口 | `sealed_` | 主队列尾 + `has_upgr_read_` |
| 唤醒互斥 | `waking_` + `retry_` | 队列锁 + 每轮重新选队首 |
| 节点链接标记 | `linked_` | 领取/取消方唯一摘链，无需标记 |
| 节点状态/waker | 每节点一把 `SpinLock` | 读者：AtomicU8 状态机；写者：队列锁 |
| 读者链 | 头/尾 `Cell`，插入序 | 原子尾指针 + `prev_` 的 LIFO 链 |
| 读者领取 | 无锁 CAS | 无锁 CAS + 原子状态迁移 |
| 读者取消/注册 | 队列锁 | 无锁原子状态机 |

## 6. 验证

- `cargo test -p atomic_sync`：87 个单测 + 2 个 cooperative_arena 集成测试
  + 7 个 cooperative 集成测试 + 8 个 preemptive 测试 + 4 个文档测试全部通过。
- `cargo clippy -p atomic_sync --all-targets`：无告警。
- `cooperative_arena` 库测试连续 20 轮压力运行通过。
- Miri（`-Zmiri-strict-provenance`）：8 个 core 单测全部通过。
- 新增/调整的 core 单测：
  - `admitted_reader_cancel_decrements_count`：已放行未领取读者取消时直接
    减计数并标取消；
  - `upgrade_request_is_inserted_and_admitted`：升级节点只在请求时插入，
    并可在组计数为 1 时直接领取或唤醒领取；
  - 其余大链批量回收、链中间取消、可升级读先释放、释放回退等测试保留。

## 7. 代价与后续

- 读者领取、取消、waker 注册现在都需要取队列锁；旧设计在这些路径上尽量
  避免队列锁。这是用**更简单的并发记账**换取的取舍，需在基准中确认高并发
  读者场景的吞吐/尾延迟是否可接受。
- `Waker::clone` 与 waker drop 现在发生在队列锁内；`Waker::wake` 仍在锁外。
- 唤醒批次上限为 32；大读者链会分多轮取锁，但每轮不分配堆内存。
- 后续可补一个 Arena 版专用基准，对比简化前后的读者热路径与升级路径开销。


## 8. 读者原子路径（2026-10-23 02:25 追加）

上一版把读者状态与 waker 放在队列锁下，导致 `poll` 领取、取消、waker 注册
都要取队列锁。这一版把它们移出：

### 8.1 读者状态机

```text
Registering --(注册完成)--> Waiting --(唤醒者 CAS)--> Taking --(取走 waker)--> Admitted
     ^                          |                                                |
     |                          +------------------(取消)--> Cancelled           |
     |                                                                           |
     +----------------------------(领取失败/重新挂起)-----------------------------+
```

- `register_waker_`：只从 `Registering` / `Waiting` 进入；写 waker 后 CAS 到
  `Waiting`。如果期间节点已被放行成 `Admitted`，返回 `false`，调用方转入
  领取而不是把它降回 `Waiting`（避免丢失已累加的计数）。
- `rewait_`：领取失败时先把放行计数减一，再从 `Admitted` 重新挂起。
- `try_admit_`：唤醒者 CAS `Waiting -> Taking`，取 waker，存 `Admitted`；
  若 waker 缺失则回退 `Waiting`（不应发生，带 `debug_assert`）。
- `cancel_`：CAS 到 `Cancelled`；`Waiting` 时清 waker，`Taking` 时自旋等
  唤醒者完成。取消是持有者自己的 future Drop，不需要队列锁。
- `mark_acquired_`：领取成功后存 `Acquired`。

### 8.2 读者链 LIFO

`RootRead` 只保存 `tail_: AtomicPtr<WaitNode>`；新读者：

```text
new.prev = tail.load()
CAS(tail, old, new)
```

读者链从 `tail` 沿 `prev` 反向遍历。发布在队列锁内完成（与唤醒遍历互斥），
所以遍历期间链稳定；但“压栈”本身是一次 CAS。

### 8.3 队列锁还剩什么

- Arena 节点分配（Arena 本身由队列锁保护）；
- 主队列的入队/摘链、写者节点状态与 waker；
- 唤醒遍历（选队首、批量 `try_admit_`、取 waker）与空组回收。

读者领取、取消、重新注册不再取队列锁；只有 `Registering`/`Taking` 的极短
重叠会让某一方做一次 `spin_loop`，这不是队列锁，也不覆盖主队列。

### 8.4 验证

- `cargo test -p atomic_sync`：87 个单测 + 2 个 Arena 集成 + 7 个 cooperative
  集成 + 8 个 preemptive + 4 个文档测试全部通过；
- `cargo clippy -p atomic_sync --all-targets` 无告警；
- `cooperative_arena` 库测试连续 20 轮压力通过；
- Miri（`-Zmiri-strict-provenance`）：8 个 core 单测通过。
