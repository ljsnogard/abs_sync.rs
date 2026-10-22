# cooperative_arena：WAKING 独占读者链与锁外整链唤醒

分支：`feat/simp-read`
基线：`f95b03f`（双实现基准与 simp-read 结论）
日期：2026-10-23
目标：消除 `release` 阶段 O(N²/B) 的“分批 + 每批重扫”，改为
**WAKING 位独占读者链 + 锁外一次遍历整链**。

## 1. 背景

上一轮基准（`bench-results-simp-read-20261023-0330.md` §4）测得：

| N | heap release | arena release（分批 32） | arena/heap |
|---:|---:|---:|---:|
| 1024 | 25.0 µs | 236.7 µs | 9.47 |
| 2048 | 91.4 µs | 1,152 µs | 12.6 |
| 4096 | 105.3 µs | 4,393 µs | 41.7 |

根因不是“批大小 32”本身，而是 `admit_group_batch_` **每批都从
`RootRead.tail_` 重扫已放行节点**，复杂度 O(N²/B)；同时每批都要重取队列锁。
只把 `WAKE_BATCH` 调到 256 仍比堆版慢约 6 倍。

## 2. 设计：WAKING 是读者链拓扑的独占锁

`RootRead::state_` 现在有三个区：

```text
bit N-1     UPGR     当前是否有已领取的可升级读持锁者
bit N-2     WAKING   读者链拓扑正被唯一唤醒者独占
bit 0..N-3  COUNT    已放行且尚未释放的读者数（含未领取）
```

`WAKING` 保护的是**链拓扑与节点生命期**：

- `tail_` 与每个节点的 `prev_`；
- 组内节点的回收（`free_group_`）。

它**不保护**节点状态：`Waiting` / `Cancelled` / `Admitted` 仍由每个
`ReaderNode` 的原子状态机并发推进，取消与领取不需要等 WAKING。

### 2.1 入队先决条件

`enqueue_reader_` 只在以下条件全部成立时把新节点压入当前组：

```text
主队列尾是 RootRead
且 WAKING == 0
且（kind == Read 或链中还没有 UpgrRead）
```

否则新建一个 `RootRead` 组追加到主队列尾。**拿不到 WAKING 时不等待**：
在持有队列锁时自旋等 WAKING，会与“唤醒者在锁外 `wake` → 被唤醒 future
同步 poll → 再取队列锁”形成死锁，所以选择“新建组”而不是阻塞。

`push_reader_` 内另有 `debug_assert!(!waking_())` 兜底。

### 2.2 选择与唯一唤醒者

`select_head_` 选中队首 `RootRead` 时：

```text
if root.waking_() { return None; }        // 已有唤醒者，本次 pass 直接退出
if !root.try_set_waking_() { return None; } // CAS 0->1，只有成功者遍历
return Some(Selection::Group(front));
```

设置 WAKING 的 CAS 在队列锁内完成，因此同一时刻只有一个线程能成功。它成为
唯一唤醒者，负责整链遍历、清 WAKING 和结束重推进；其他 `pass` 看到 WAKING
直接返回，**不需要旧版的 `retry_`**。

### 2.3 锁外整链遍历

`wake_group_` 在释放队列锁之后执行：

```text
从 tail_ 沿 prev_ 走完整条链：
    若节点是 Waiting：
        CAS Waiting -> Taking，取 waker，存 Admitted
        root.inc_count_()
        waker.wake()
    若节点是 UpgrRead 且升级进行中：跳过
遍历结束：
    root.clear_waking_()
    返回本轮放行数
```

因为 WAKING 独占链拓扑：

- 新读者不会压入本组（§2.1），`tail_`/`prev_` 完全静止；
- `prune_front_` 看到 WAKING 直接 return，节点不会被回收；
- `Waker` 的取出、`clone`/drop 和 `wake()` 全部在锁外。

### 2.4 prune 尊重 WAKING

```text
if root.waking_() { return; }   // 不回收，也不越过队首组
if root.count_() == 0 && !root.has_pending_() { free_group_ }
```

这同时封住两个 UAF 窗口：

1. 全部 `Waiting` 被取消后 `count == 0 && !has_pending`；
2. `try_admit_` 已 CAS 到 `Taking`、尚未 `inc_count_` 的瞬间
   （`has_pending_` 对 `Taking` 返回 false）。

### 2.5 结束重跑（唯一唤醒者的义务）

后来者在 WAKING 期间看到的入队、取消、写者释放都会被丢弃，所以唯一唤醒者
在 `clear_waking_()` 后必须重新进入 `run_pass_` 循环：

- 若还有 pending（遍历期间新压栈的节点、`Registering`/`Taking`、被 skip 的
  `UpgrRead`、后继写者），再次 `select`、置 WAKING、遍历；
- 若本轮放行数为 0（只剩 `Registering`/`Taking`），退出，让这些节点的
  所有者自行触发下一次 pass，避免忙循环；
- 若重跑时 WAKING 已被另一个线程抢到，退出，由新唤醒者兜底。

### 2.6 与旧版机制的对应

| 旧版机制 | 现在 |
|---|---|
| `sealed_` 防新读者合并 | 入队先决条件 `WAKING == 0` |
| `waking_` 防遍历期间回收 | `prune_front_` 尊重 WAKING |
| `retry_` 处理唤醒者竞争 | CAS 唯一唤醒者 + 结束重跑 |
| 队列锁内收集/分批唤醒 | 锁外整链遍历、逐个 `wake` |

因此 `WakerBatch`、`WAKE_BATCH`、`admit_group_batch_`、`wake_selection_`
全部删除；稳态仍然 0 堆分配（不需要 waker 容器）。

## 3. 代码改动

- `wait_node_.rs`：
  - 新增 `WAKING_BIT`，`COUNT_MASK` 排除 `UPGR | WAKING`；
  - `waking_` / `try_set_waking_` / `clear_waking_`；
  - `inc_count_` 加 debug 断言，固定“计数不会进位到标志位”的前提；
  - `push_reader_` 加 WAKING 断言与注释。
- `core_.rs`：
  - `prune_front_` 尊重 WAKING；
  - `select_head_` 置 WAKING、已置位返回 `None`；
  - `wake_group_` 锁外整链遍历；
  - `take_write_waker_` 锁内取写者 waker；
  - `run_pass_` 改为 `WakeAction::{Stop, Group, Write}` 循环；
  - `enqueue_reader_` 加 `!waking_()` 先决条件；
  - 删除 `WAKE_BATCH`、`WakerBatch`、`admit_group_batch_`、`wake_selection_`。
- `mod.rs`：更新结构说明。
- 新增单测 `waking_group_is_exclusive_for_enqueue_and_prune`：
  验证 WAKING 期间新读者新建组、空组不被 prune 回收，清除后恢复推进。

## 4. 验证

- `cargo test -p atomic_sync`：
  88 单测 + 2 个 cooperative_arena 集成 + 7 个 cooperative 集成
  + 8 个 preemptive + 4 个文档测试全部通过。
- `cargo clippy -p atomic_sync --all-targets`：无告警。
- `format_use_imports.py --check`：合规。
- 双实现 bench 全场景（micro 10 轮、release-threads 10 轮、concurrent 10 轮、
  memory 5 轮）无挂起、无 panic。

## 5. bench 复测（10 轮中位数）

`release` = 释放写者到全部读者被唤醒的耗时（arena 侧现在是锁外整链遍历）：

| N | heap | 旧 arena（分批 32） | 新 arena（WAKING） | 新/旧 | 新/heap |
|---:|---:|---:|---:|---:|---:|
| 32 | 6,861 | 1,554 | 1,212 | 0.78 | 0.18 |
| 128 | 5,897 | 7,334 | 4,276 | 0.58 | 0.73 |
| 512 | 12,628 | 58,778 | 18,704 | 0.32 | 1.48 |
| 1024 | 24,922 | 236,729 | 38,684 | 0.16 | 1.55 |
| 2048 | 97,702 | 1,152,335 | 81,790 | 0.071 | 0.84 |
| 4096 | 108,754 | 4,392,802 | 165,904 | **0.038** | 1.53 |

`drain` = 释放写者到全部读者领取完成：

| 指标 | 旧 arena | 新 arena | heap |
|---|---:|---:|---:|
| reader-wake N=4096 drain | 4,897,464 | **653,828** | 77,916,963 |
| reader-wake N=4096 per_waiter | 1,195 | **160** | 19,022 |
| deep N=4096 total | 5,452,657 | **1,624,708** | 103,089,420 |
| deep N=4096 drain | 4,253,664 | **417,594** | 77,725,270 |
| deep N=4096 drop | 196,408 | 199,822 | 71,346 |
| reader-release N=64 release_all | 2,104 | 2,104 | 978 |

其他场景：

| 场景 | 旧 arena | 新 arena | heap |
|---|---:|---:|---:|
| cancel 0% drain | 83,146 | 41,690 | 762,986 |
| cancel 50% total | 66,232 | 61,069 | 514,930 |
| cancel 100% total | 43,766 | 44,704 | 266,466 |
| concurrent tokio ns/op | — | 446 | 694 |
| concurrent compio ns/op | — | 62 | 69.5 |
| memory N=4096 peak RSS | — | 2,728 KB | 2,544 KB |

## 6. 结论

- `release` 从 **4.39 ms 降到 166 µs（26×）**，与堆版一次收集的 109 µs 只差
  1.5×；O(N²/B) 已消除，N 从 1024 到 4096 近似线性增长（38.7 → 165.9 µs）。
- 端到端仍大幅领先：reader-wake N=4096 的 `drain` 从 4.90 ms 降到 654 µs，
  deep 总耗时从 5.45 ms 降到 1.62 ms；相对堆版分别快约 **119×** 和 **63×**。
- 取消风暴、并发运行时、内存占用基本不变（`drop` 回收仍是 Arena 的固定
  成本，约为堆版的 2.8×）。
- 删除 `WAKE_BATCH` 后不再需要“批大小调参”；`release` 的剩余 1.5× 差距来自
  Arena 每个读者两次原子 RMW（`try_admit_` + `inc_count_`）与逐个 `wake`，
  而堆版是在队列锁内一次性收集后批量 `wake`。这是可接受的常数差。

## 7. 下一步

1. 如需把 `release` 的 1.5× 常数差也压掉，可评估“锁内只标 `Admitted` +
   `inc_count`，锁外只取 waker/wake”，或把 `inc_count` 合并进
   `try_admit_` 的一次 CAS。
2. 把 `WAKING` 的“结束后重跑”加入并发压力测试（tokio 固定 worker 下的
   reader-wake/cancel），确认高争用下无丢唤醒或长尾。
3. 跑 Miri（`-Zmiri-strict-provenance`）覆盖新的锁外遍历路径：
   重点是 `wake_group_` 与 `enqueue_reader_` 的 WAKING 分支。
4. `CHUNK_CAPACITY=256` 的冷启动收益仍可单独提交。
