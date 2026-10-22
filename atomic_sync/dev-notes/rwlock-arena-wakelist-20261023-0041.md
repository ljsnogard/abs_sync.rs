# Arena 版协作式读写锁：WaitNode / WakeNode 唤醒链重构设计

## 1. 目标与已确认决策

把 `cooperative_arena` 的等待队列从“读类节点 + 槽位链”改成“统一 WaitNode +
WakeNode 内容枚举”，并利用 RootRead 的持锁计数决定读者链的批量唤醒、升级插队
与批量回收。

已与需求方确认：

1. 删除 `cooperative-arena` feature，`cooperative` 与 `cooperative_arena` 同时
   编译、对外 API 完全一致（已完成，提交 `dcfe422`）。
2. `ArenaWaitNode` 改名为 `WaitNode`；所有节点共用 `prev_` / `next_` 兄弟指针，
   节点内容统一为 `enum WakeNode`。
3. 四种内容变体：`RootRead`、`PureRead`、`UpgrRead`、`PureWrite`。
4. `PureRead` / `UpgrRead` 可混在一条读者链中，一条链最多一个 `UpgrRead`。
5. `RootRead.count_` 表示**当前持锁读者数**，读者释放时减一（不是唤醒时减一）。
6. `UpgrRead` = `WaitKind::UpgradableRead` 等待者；`Upgrade` 请求新分配一个
   `PureWrite`（带 upgrade 标记）并迁移升级句柄。

## 2. 数据结构

```rust
struct WaitNode {
    prev_: Option<NonNull<WaitNode>>, // 主队列前驱；读者链节点为 None
    next_: Option<NonNull<WaitNode>>, // 主队列后继，或读者链后的下一个读者
    content_: WakeNode,
}

enum WakeNode {
    RootRead(RootRead),
    PureRead(ReaderNode),
    UpgrRead(ReaderNode),
    PureWrite(WriteNode),
}

struct RootRead {
    /// 本组已放行、尚未释放的读者数；原子递减，避免每次释放都取队列锁。
    count_: AtomicUsize,
    /// 读者链头/尾；只在持有队列锁时修改。
    head_: Option<NonNull<WaitNode>>,
    tail_: Option<NonNull<WaitNode>>,
    /// 封口后不再接受新的读类等待者。
    sealed_: bool,
    /// 唤醒者正在遍历本组；阻止节点被回收。
    waking_: bool,
}

struct ReaderNode {
    /// 所属 RootRead，释放时据此递减 count_。
    root_: NonNull<WaitNode>,
    /// 槽位状态与 waker，由节点锁保护。
    slot_: SpinLock<ReaderSlot>,
}

struct ReaderSlot {
    kind_: WaitKind,          // Read 或 UpgradableRead
    state_: SlotState,        // Waiting / Admitted / Acquired / Cancelled
    waker_: Option<Waker>,
}

struct WriteNode {
    kind_: WaitKind,          // Write 或 Upgrade
    /// Upgrade 节点记录自己对应的读者组；Write 为 None。
    root_: Option<NonNull<WaitNode>>,
    slot_: SpinLock<WriteSlot>, // waker / state
}
```

### 2.1 主队列与读者链

```text
主队列（prev_/next_）: RootRead ──> PureWrite ──> RootRead ──> ...
                         │
读者链（next_ 单链）:     PureRead ──> PureRead ──> UpgrRead（至多一个）
```

- 主队列承载读者组和独占等待者；读者组只要 `count_ > 0` 就**一直留在主队列**。
  不能因为“等待者已全部唤醒”就摘除：可升级读可能在升级为写者后又降级回可升级
  读甚至普通读，组的持锁计数必须连续跟踪。只有 `count_ == 0` 且没有待领取/
  等待节点时，最后一个读者才把整组摘链并批量回收。
- 读者链不逐个摘链：取消/已领取的节点只改状态，统一在 `count_` 归零时批量回收。

### 2.2 WaitKind 映射

| WaitKind | 队列节点 | 位置 |
|---|---|---|
| `Read` | `PureRead` | 读者链 |
| `UpgradableRead` | `UpgrRead` | 读者链，至多一个 |
| `Upgrade` | `PureWrite(kind = Upgrade)` | 主队列，插到队首 |
| `Write` | `PureWrite(kind = Write)` | 主队列队尾 |

## 3. 不变量

1. 一条读者链最多一个 `UpgrRead`；出现 `UpgrRead` 后该组立即封口。
2. 主队列中 `RootRead` 的先后顺序等于读者组到达顺序；`PureWrite` 的先后顺序
   等于写者/升级请求的放行顺序。
3. `count_` 只统计**从本组队列中放行**的读者；快速路径读者不计入任何组。
4. `count_ > 0` 时，`RootRead` 及其读者链中所有节点都必须保持存活。
5. `count_ == 0` 且读者链已无 `Waiting` 节点时，组才能被回收。
6. 队列锁只保护主队列拓扑与组内 `head_/tail_/sealed_/waking_`；读者领取仍由
   原子状态字上的 CAS 完成，不在队列锁内放行许可。
7. 任何自旋锁内不调用 `Waker::wake`。

## 4. 入队

- `Read`：
  - 若主队列尾是未封口的 `RootRead`，把新的 `PureRead` 追加到该组读者链尾；
  - 否则新建 `RootRead`，追加到主队列尾，再挂 `PureRead`。
- `UpgradableRead`：
  - 若主队列尾是未封口且链中还没有 `UpgrRead` 的 `RootRead`，追加 `UpgrRead`
    并立即封口该组；
  - 否则新建 `RootRead` 组，挂 `UpgrRead` 并封口。
- `Write`：新建 `PureWrite(Write)` 追加到主队列尾；若原队尾是 `RootRead`，
  封口该组（新读者不能再插到写者前面）。
- `Upgrade`：有读者组（从队列放行的可升级读）时，新建 `PureWrite(Upgrade)`
  插到对应 `RootRead` 的**后继**；无读者组（快速路径可升级读）时插到主队列
  **队首**（维持现有防死锁语义）。两种情况都计入 `writer_waiters_` /
  `WRITER_QUEUED`。

入队与“是否队首 + 尝试领取 + 注册 waker”仍在同一段队列锁临界区内完成，避免
丢唤醒。

## 5. 唤醒 / 推进

`run_pass_()` 循环：

1. 取队列锁，`prune_front_`：回收已无待决节点、`count_ == 0` 的前置 `RootRead`
   与已取消/已领取的 `PureWrite`。
2. 选择队首：
   - `RootRead`：
     - 有写者持有 → 不放行；
     - 否则设 `sealed_ = true`、`waking_ = true`，把整条读者链交给锁外唤醒；
   - `PureWrite(Write)`：`READER_COUNT == 0 && !writer_active && !upgrade_active`
     时放行；
   - `PureWrite(Upgrade)`：`READER_COUNT == 1 && upgrade_active && !writer_active`
     时放行。
3. 锁外唤醒被选中的节点：
   - 读者链：按链序逐个取 waker、置 `Admitted`，并对每个成功放行的读者
     `count_.fetch_add(1)`；
     - `UpgrRead` 若当前 `UPGRADE_ACTIVE` 已置位，则保持 `Waiting`，等下一个组；
   - 独占节点：取 waker、置 `Admitted`；
   - 每个 waker 在释放节点锁后再 `wake()`。
4. 回到 1，直到没有可放行节点。

`RootRead` 的读者链全部放行后：
- 若 `count_ > 0`，`RootRead` **继续留在主队列**，位于队首；它不再接收新
  读者，但作为活跃组的根，直到 `count_` 归零才由最后一个读者摘链并批量回收；
- 若链中还有因 `UPGRADE_ACTIVE` 被挡住的 `UpgrRead`，该组同样留在主队列，
  后续 pass 会再次尝试放行它；
- 若 `count_ == 0` 且链中已无 `Waiting` 节点（全部取消/未领取），直接批量回收。

活跃 `RootRead` 位于队首时，`select_head_` 允许查看它的后继：
- 后继是已激活的 `PureWrite(Upgrade)` 且 `READER_COUNT == 1` 时，直接放行该
  升级写者；
- 否则停止，让普通写者等 `READER_COUNT == 0`。

## 6. 读者释放与 count_

`ReaderGuard` / `UpgradableReaderGuard` 增加私有字段 `root_: Option<NonNull<WaitNode>>`：

- 快速路径获得 → `None`；
- 队列放行后获得 → `Some(root)`；
- 降级（`Upgradable -> Read`）保留 root；写者降级得到的读者为 `None`。

释放时：

1. 无条件执行原子状态迁移（`release_reader` / `release_upgradable_read`）。
2. 若 `root_ == None`：直接 `run_pass_()`（与现实现一致）。
3. 若 `root_ == Some(root)`：
   - `old = count_.fetch_sub(1)`；
   - `old > 2`：只递减，不取队列锁；
   - `old == 2`（减到 1）：若该组读者链中存在 `UpgrRead`，进入 §7 的升级
     插队处理；随后 `run_pass_()`；
   - `old == 1`（减到 0）：本线程是最后一个读者，取队列锁把 `RootRead` +
     整条读者链（以及该组预留的 Upgrade 节点）批量归还 Arena，然后
     `run_pass_()`。

## 7. 升级插队

`Upgrade` 请求对应一个 `PureWrite(kind = Upgrade)`。它有两种来源：

1. **有读者组**（可升级读者是从队列里放行的）：升级节点挂到该组 `RootRead`
   的后继位置，并记录 `root_ = Some(组)`；这样它排在同组读者之后、普通写者之前。
2. **无读者组**（可升级读者走快速路径获得）：升级节点插到主队列队首，维持
   现有防死锁语义。

当组 `count_` 减到 1 且链中有已领取（`Acquired`）的 `UpgrRead` 时，说明只剩
可升级读者持锁，升级时机已到：若该组还没有升级节点，就在 `RootRead` 后继
**预分配一个休眠的 `PureWrite(Upgrade)`**（`state = Reserved`，不注册 waker、
不计入 `writer_waiters_`、不置 `WRITER_QUEUED`）。它只是占住“下一个待激活写者”
的位置，不影响新读者快速路径。

可升级读守卫调用 `upgrade_async()` 时：
- 若组里已有休眠占位节点，直接接管它（写入 waker、转入 `Waiting`、置
  `WRITER_QUEUED`）；这就是“迁移升级句柄”；
- 若没有占位节点（例如 `count_ > 1` 时就发起升级），退回到在组后继新建
  `PureWrite(Upgrade)` 的路径；
- `try_upgrade_now()` 仍走状态字快速路径；成功后休眠占位节点保持 `Reserved`，
  等组结束时随批量回收释放。

`run_pass_()` 只放行 `state = Waiting/Admitted` 的升级节点；`Reserved` 节点被
跳过。升级节点被领取（`Acquired`）后即可从主队列摘除并归还 Arena；它不再需要
等待组结束。

## 8. 取消

- `Waiting` 读者取消：
  - 只把槽位标 `Cancelled`，不从链中摘除；
  - 若该组还没有被放行，`run_pass_()` 会跳过它；
  - 若该组已经放行但该槽位还没被领取（`Admitted`），必须 `count_.fetch_sub(1)`，
    并按 §6 的 `old == 2 / old == 1` 分支处理。
- `Waiting` 独占取消：
  - 标 `Cancelled`，`writer_waiters_ -= 1`，归零时清 `WRITER_QUEUED`；
  - 若节点位于队首，`prune_front_` 后继续推进。
- `Admitted` 独占取消：
  - 独占节点没有 count_，只清计数并推进；若已被放行但未领取，许可由状态字
    CAS 保证不会泄漏（现有语义）。

## 9. 批量回收

- 最后一个读者释放时，本线程持有队列锁：
  1. 确认 `count_ == 0`；
  2. 若 `RootRead` 仍在主队列中，摘链；
  3. 遍历读者链，把所有 `PureRead` / `UpgrRead` 节点（含已 `Cancelled` /
     `Acquired` 的）整体 `free` 回 Arena；
  4. 若该组有预留/关联的 `PureWrite(Upgrade)`，一并摘链并释放；
  5. 释放 `RootRead` 自身；
  6. 释放队列锁后 `run_pass_()`。
- 回收时节点里的 `Waker` 由 `free` 返回的 `ArenaItem` 整块 drop；不做逐节点
  摘链，也不为单个节点实现 `Drop`。

## 10. 已确认决策

1. **count 减到 1 且无升级请求时创建休眠预留写节点**：是。按原描述在
   `RootRead` 后继预分配 `PureWrite(Upgrade)`，守卫真正升级时接管；未升级则
   随组批量回收。
2. **活跃 RootRead 保留在主队列**：是。读者释放只做原子减，`count_ == 0` 的
   最后一次释放才取队列锁摘链、批量回收。
3. **升级节点位置**：有组时挂在对应 `RootRead` 之后；无组时插到队首。

## 11. 测试计划

- 现有 `cooperative_tests_.rs` 全量行为测试（compio 单线程 + tokio 多线程）
  必须两套实现都通过。
- 新增：
  1. 大读者组 + 最后一个读者释放后批量回收，断言队列为空、Arena 长度归零；
  2. `PureRead` + `UpgrRead` 混合链，升级请求在 count 减到 1 后被放行；
  3. 读者链中间节点取消，最后一个读者释放仍能回收整条链；
  4. 升级请求取消后 `WRITER_QUEUED` 归零，新读者恢复快速路径；
  5. 组内所有读者取消，`RootRead` 能被 prune 并回收；
  6. Miri（strict provenance）覆盖新队列的链接与批量回收。
- 单测超时 ≤ 1 分钟；全量测试超时 ≤ 5 分钟。


## 12. 实施记录（2026-10-23）

已按本设计完成 `cooperative_arena` 重构：

- `WaitNode { prev_, next_, content_: WakeNode }`，四种内容变体
  `RootRead` / `PureRead` / `UpgrRead` / `PureWrite`；
- 主队列 = `RootRead` + `PureWrite`，读者链独立单链、至多一个 `UpgrRead`；
- `RootRead.count_` 在读者领取时加一、释放时减一；`upgr_active_` 单独跟踪
  当前是否有已领取的可升级读持锁者；
- `count_` 减到 1 且 `upgr_active_` 时，在组后继预分配休眠
  `PureWrite(Upgrade)`；升级请求接管它并把升级句柄迁移过去；
- `count_` 减到 0 时，最后一个读者取一次队列锁，整条读者链 + 组根 +
  预留节点批量归还 Arena；不逐节点摘链，也没有自定义 `Drop`；
- **`RootRead` 在 `count_ > 0` 期间始终留在主队列**。早期文档写过
  “等待者唤醒完就摘除”，这是错误的：可升级读可能升级为写者再回退为可升级读
  甚至普通读，组必须连续跟踪持锁者，直到 `count_` 归零才摘链。

并发收尾细节：

- 读者领取/释放不取队列锁；`count_`、`pending_`、`upgr_active_`、
  `release_ops_` 都是原子量；
- `release_ops_` 记录正在进行释放收尾的线程数。2→1 的线程可能在 `count_`
  减一后还要访问组做升级预留，因此 1→0 的线程不能立刻回收；必须等所有
  释放收尾结束，由队首 `prune_front_` 统一批量回收；
- 守卫只携带一个 `tracked: bool`，核心用 `active_read_group_` 定位当前活跃
  组（同一时刻至多一个组持有读者）；
- 写者/升级节点的领取、取消由**状态转换方自己**在队列锁内释放节点；
  `prune_front_` 不再抢先释放终态节点，避免“状态已改、尚未收尾”的重复释放；
- 唤醒读者组时仍用 `waking_` / `retry_` 防止锁外遍历期间的丢唤醒。

验证结果：

- `cargo test -p atomic_sync`：85 个单测 + 7 个 cooperative 集成测试 +
  2 个 cooperative_arena 集成测试 + 8 个 preemptive 测试 + 4 个文档测试
  全部通过；
- `cooperative_arena` 库测试连续 25 轮压力运行通过；
- `cargo miri test`（`-Zmiri-strict-provenance`）：6 个 core 单测与 8 个
  Arena 池单测全部通过；
- `cargo clippy -p atomic_sync --all-targets` 无告警（共享测试文件对
  `clippy::duplicate_mod` 显式豁免）；
- 新增测试：跨 chunk 大读者链批量回收（1100 节点）、链中间取消、升级占位
  生成/接管、可升级读先于普通读释放、升级 → 回退 → 降级为普通读的组存活；
- 两套实现的 `release_upgradable_read` 都增加了“按普通读回退”分支：当
  可升级读守卫已被并行的写者降级路径转成普通读时，释放不会泄漏读者计数；
- 新增 `tests/cooperative_arena_rwlock.rs`：只通过公开 API / `abs_sync` trait
  驱动 Arena 版，覆盖一轮读改写升级降级与多线程读写互斥。

已知边界：

- `WriterGuard::downgrade_to_upgradable()` 作用在“由升级得到的写者”上时，
  沿用原有状态机的语义（会再次置 `UPGRADE_ACTIVE`、增加读者计数）。这是
  既有 API 行为，本次等待队列重构没有改变它；普通写者的降级路径不受影响。
