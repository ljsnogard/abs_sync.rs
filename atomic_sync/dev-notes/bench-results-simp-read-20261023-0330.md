# 两套 cooperative 读写锁的差异基准结果（feat/simp-read）

分支：`feat/simp-read`
HEAD：`c2ae429`（本次只新增基准设施与文档，未改核心实现）
日期：2026-10-23
对照：`rwlock::cooperative`（堆分配等待队列，下称 **heap**）与
`rwlock::cooperative_arena`（简化 Arena 队列，下称 **arena**）。

## 0. 结论摘要

1. **深队列/大读者链是 Arena 的绝对优势区**。reader-wake 与 deep-queue 在
   N≥128 后 Arena 快 5～19 倍（N=4096：drain 77.0 ms → 4.9 ms，deep 总耗时
   101.6 ms → 5.45 ms）。来源是堆版的 `Vec<WaiterSlot>` 扩容与“每次 poll 都
   取队列锁 + 扫全组”造成的二次复杂度。
2. **Arena 的批量唤醒在深队列上反而更慢，且随 N 超线性增长**：
   `release`（释放写者 → 唤醒全部读者）从 N=128 起被堆版反超，N=4096 时
   Arena 4.39 ms vs 堆版 105 µs（**慢 41.7 倍**）。原因是 `WAKE_BATCH=32`
   且每批都从链尾重扫已放行节点，复杂度 O(N²/32)。**这是本次最明确的
   可修问题**：把 `WAKE_BATCH` 调到 128/256 后，N=4096 的 release 降到
   1.14 ms / 0.66 ms（见 §7）。
3. **“Arena 多读者释放占优”的预测不成立**：单线程顺序释放 Arena 慢
   1.5～3.3 倍（N=1 3.3×、N≥16 2.1×），4 个 OS 线程同时释放仍慢
   1.55～2.6 倍。Arena 每次 `release_reader` 要做两次原子 RMW，最后一个
   释放者还要逐个归还整链节点；堆版每次只是“取一次无争用自旋锁 + 空
   pass”，单/低争用下更便宜。
4. **取消风暴是 Arena 的稳定优势区**：取消比例 ≥25% 时，取消耗时快
   6～7 倍、drain 快 9～11 倍、总耗时快 6～8 倍（取消 50%：515 µs → 66 µs）。
5. **稳态分配**：预热后 Arena 实现分配 **0 次 / 0 字节**，堆版每次
   “入队→唤醒→领取→释放”固定 **5 次 / 536 字节**。断言已验证。
6. **冷启动是 Arena 的固定成本**：每把新锁首次竞争 6.8 µs vs 堆版 0.61 µs
   （**慢 11.2 倍**），每次冷启动分配 65.6 KB chunk。把 `CHUNK_CAPACITY`
   从 1024 降到 256 后冷启动降到 1.15 µs（5.9× 改善），深队列几乎不变
   （见 §8）。建议改小 chunk。
7. **fastpath 无差异**：`try_read` 22.0 vs 22.5 ns、`try_write` 22.0 vs
   22.0 ns（≤2.3%，在阈值 3% 内）。
8. **tokio 4 worker 混合负载 Arena 快 2 倍**（788 → 400 ns/op）；compio 单
   线程持平（69 → 62 ns/op，约 10%，接近噪声）。OS 线程 `block_on` 一节
   未采用为判据。

一句话：**Arena 赢在稳态队列与取消，输在唤醒批处理与释放回收；两者都可以
用“批次游标 + 更小 chunk”进一步缩小，而深队列优势本身非常稳固。**

## 1. 环境

见 `.tmp/bench-simp-read/env.txt`：

- CPU：Intel(R) Celeron(R) J3455 @ 1.50 GHz，4 核 / 4 线程
- 频率策略：`schedutil`（`/sys/.../scaling_governor` 只读，无法固定为
  performance；实测频率在 0.8～2.3 GHz 间浮动）
- 内核：Linux 6.12.105-production+truenas x86_64
- 工具链：`rustc 1.101.0-nightly (4ebd25825 2026-10-15)`
- commit：`c2ae429647fa0af61d02a456acbebe7fd0340226`
- 绑核：单线程微基准 `taskset -c 0`；4 worker 并发场景 `taskset -c 0-3`

## 2. 基准设施（本次新增，已提交）

两个 bench target 共用一份源，避免“临时改 import 再还原”：

```text
benches/rwlock_compare_heap.rs   -> use ...::cooperative as impl_mod
benches/rwlock_compare_arena.rs  -> use ...::cooperative_arena as impl_mod
benches/common/rwlock_compare_common.rs   (include!)
benches/rwlock_alloc_heap.rs / rwlock_alloc_arena.rs
benches/common/rwlock_alloc_common.rs     (计数全局分配器 + include!)
```

每个 target 只定义 `impl_mod`、`TestLock = impl_mod::CooperativeRwLockOwned<u64>`、
`IMPL_NAME`，其余代码完全一致。`benches/common/` 放在子目录，避免 Cargo
把它自动发现成独立 target（`cargo clippy --all-targets` 会编译到它）。

### 2.1 驱动方式

除 `fastpath`/`concurrent` 外，所有场景都是**单线程手动 poll**：

- 写者先 `try_write` 持锁；
- 每个读者/写者各自一个 `CooperativeAcqSession` + `ReadAcquireFuture`，
  用 `Waker::noop()` 首次 poll 入队（返回 `Pending`）；
- 释放写者，再逐个 poll 到 `Ready` 领取；
- 各阶段用 `Instant::now()` 取值。

这样可以把“入队 / 释放写者触发唤醒 / 领取 / 释放回收”拆开计时，且不受
运行时调度噪声影响。noop waker 也意味着**没有运行时 Waker 分配**，分配
计数里所有非测量装置分配都来自锁实现本身。

每轮开始先 `warm_lock_`（一个小写者+读者完整流程）把 Arena 首次 chunk
分配移出测量区间；`cold` 场景是唯一例外，它测的就是冷启动。

### 2.2 统计口径

- 每个配置跑 10 轮，**每轮一个独立进程，heap/arena 交替**；报告
  **中位数**，并给出 10 轮样本的 p95/p99（不是单轮内 p95）。
- `reader-wake` 的 `per_waiter` = `drain`（释放写者到全部读者领取完成）
  ÷ N；`release` 只算 `drop(writer_guard)` 内的唤醒批次。
- `deep` 额外记录 `drop`（guard 释放回收）与 RSS/VmHWM。
- `reader-wake` 在 N≤256 时还记录“从释放起算的累计领取时刻”的 p50/p95/p99，
  它是**排空进度**而不是单读者独立延迟，故本文主要用轮级统计。

### 2.3 命令

```sh
# 构建
cargo bench -p atomic_sync --bench rwlock_compare_heap \
    --bench rwlock_compare_arena --bench rwlock_alloc_heap \
    --bench rwlock_alloc_arena --no-run

# 单线程微基准组（每轮独立进程，交替跑 10 轮）
taskset -c 0 env RWLCMP_ROUNDS=1 \
  RWLCMP_SCENARIOS=fastpath,reader-wake,reader-release,deep,deep-mixed,cancel,upgrade \
  RWLCMP_CANCEL_K=512 <bin> >> heap_micro.log

# 并发释放组
taskset -c 0-3 env RWLCMP_ROUNDS=1 RWLCMP_SCENARIOS=release-threads <bin>

# 冷启动 / 内存 / 运行时并发
taskset -c 0 env RWLCMP_ROUNDS=1 RWLCMP_SCENARIOS=cold RWLCMP_COLD_K=2000 <bin>
taskset -c 0 env RWLCMP_ROUNDS=1 RWLCMP_SCENARIOS=memory RWLCMP_NS=4096 RWLCMP_BURSTS=3 <bin>
taskset -c 0-3 env RWLCMP_ROUNDS=1 RWLCMP_SCENARIOS=concurrent \
  RWLCMP_CONC_N=4 RWLCMP_CONC_OPS=5000 RWLCMP_CONC_WORKERS=4 <bin>

# 分配计数（自带断言；Arena 断言 0 分配，堆版断言 >0）
taskset -c 0 env RWLCMP_ALLOC_ROUNDS=3 RWLCMP_ALLOC_K=200 \
  RWLCMP_ALLOC_WARM=64 RWLCMP_ALLOC_N=8 RWLCMP_ALLOC_COLD_K=100 \
  RWLCMP_ALLOC_DEEP_N=4096 <alloc-bin>
```

完整驱动脚本与原始输出在 `.tmp/bench-simp-read/`（不提交）：
`run_all.sh`、`run_micro.sh`、`analyze.py`、`heap_*.log`、`arena_*.log`、
`alloc_*.log`、`wake_batch_*.log`、`chunk_*.log`、`analysis.txt`、
`summary.csv`、`tables.md`。

## 3. fastpath（对照）

单线程，20 万次 `try_read` / `try_write`，10 轮中位数：

| 操作 | heap (ns/op) | arena (ns/op) | arena/heap |
|---|---:|---:|---:|
| try_read | 22.0 | 22.5 | 1.023 |
| try_write | 22.0 | 22.0 | 1.000 |

**无显著差异**（≤2.3%，在 3% 阈值内）。两版快速路径本来就共用同一套状态字
CAS 逻辑。

## 4. reader-wake（写者持锁 → N 读者排队 → 释放 → 排空）

`per_waiter` = drain / N（ns，越小越好）：

| N | heap | arena | arena/heap |
|---:|---:|---:|---:|
| 1 | 762 | 1,168 | 1.53 |
| 2 | 694 | 585 | 0.84 |
| 4 | 389 | 306 | 0.79 |
| 8 | 382 | 358 | 0.94 |
| 16 | 322 | 197 | 0.61 |
| 31 | 336 | 452 | 1.35 |
| 32 | 504 | 162 | **0.32** |
| 33 | 538 | 177 | **0.33** |
| 64 | 488 | 160 | **0.33** |
| 128 | 603 | 185 | **0.31** |
| 512 | 1,566 | 275 | **0.18** |
| 1024 | 3,545 | 356 | **0.10** |
| 2048 | 9,646 | 680 | **0.07** |
| 4096 | 18,788 | 1,195 | **0.06** |

`release`（`drop(writer)` 内的唤醒批量，ns）：

| N | heap | arena | arena/heap |
|---:|---:|---:|---:|
| 32 | 6,320 | 1,554 | 0.25 |
| 64 | 7,119 | 3,483 | 0.49 |
| 128 | 6,082 | 7,334 | 1.21 |
| 512 | 12,261 | 58,778 | **4.79** |
| 1024 | 24,994 | 236,729 | **9.47** |
| 2048 | 91,402 | 1,152,335 | **12.6** |
| 4096 | 105,329 | 4,392,802 | **41.7** |

解读：

- N≥32 后 Arena 的**总排空**领先 3～16 倍，N 越大优势越大；N≤8 两版接近
  （Arena 在 N=1 慢 1.5 倍，绝对差 ~0.4 µs，属小 N 噪声）。
- **批次边界 31/32/33 没有稳定阶跃**。N=31 的一次“Arena 慢 1.35×”经反转
  N 顺序实验证明是**测量位置效应**：同一二进制把 `NS=32,31` 反过来跑，
  N=31 立刻从 7.0 µs 变 15.1 µs、N=32 从 17.2 µs 变 7.1 µs。小 N 的绝对
  差值在 1～20 µs 量级，受进程内首次测量/分配布局影响，**不作为判据**。
- `release` 阶段从 N=128 起被堆版反超、并随 N 超线性增长，是
  `WAKE_BATCH=32` + 每批重扫的直接证据（§7 实验）。

## 5. reader-release（N 个读者领取后释放）

### 5.1 单线程顺序释放

| N | heap (ns) | arena (ns) | arena/heap |
|---:|---:|---:|---:|
| 1 | 162 | 538 | **3.31** |
| 2 | 156 | 331 | 2.13 |
| 4 | 270 | 398 | 1.48 |
| 8 | 257 | 492 | 1.92 |
| 16 | 358 | 731 | 2.04 |
| 31 | 552 | 1,152 | 2.09 |
| 32 | 550 | 1,172 | 2.13 |
| 33 | 568 | 1,202 | 2.12 |
| 64 | 978 | 2,104 | 2.15 |

### 5.2 4 个 OS 线程同时释放（原子开关同时放行）

| N | 指标 | heap | arena | arena/heap |
|---:|---|---:|---:|---:|
| 2 | median | 420 | 1,089 | 2.60 |
| 2 | sum | 626 | 1,314 | 2.10 |
| 4 | median | 319 | 496 | 1.55 |
| 4 | sum | 1,262 | 2,474 | 1.96 |

**预测不成立**：无论顺序还是并发，Arena 的释放都更慢。原因是：

- Arena 每个释放者要做 `state_.release_reader()` 和 `RootRead.dec_count()`
  两次原子 RMW；最后一个还要沿链 `free_node_` 逐个归还整组节点（N=64 时
  一次回收 65 个节点）。
- 堆版每个释放者只是“状态字一次原子 + 取一次无争用自旋锁 + 空 `pass_`”，
  最后一个释放时 `Vec<WaiterSlot>` 一次性 drop。单线程/低争用下锁很便宜。

这也与 deep 的 `drop` 列一致：Arena 慢 2.3～2.8 倍。

## 6. deep-queue / deep-mixed

### 6.1 deep（纯读者，写者持有 → N 排队 → 释放 → 排空）

| N | heap total (ns) | arena total (ns) | arena/heap | heap drop | arena drop |
|---:|---:|---:|---:|---:|---:|
| 1 | 1,898 | 2,153 | 1.13 | 137 | 427 |
| 128 | 111,431 | 38,021 | 0.34 | 1,874 | 4,289 |
| 512 | 1,090,063 | 181,958 | 0.17 | 7,712 | 19,261 |
| 1024 | 4,611,518 | 559,910 | 0.12 | 14,745 | 38,683 |
| 2048 | 25,487,568 | 1,586,750 | 0.062 | 32,869 | 77,791 |
| 4096 | 101,622,954 | 5,452,657 | **0.054** | 69,228 | 196,408 |

`enqueue` 同样从 N=128 起 Arena 快 2～26 倍（N=4096：25.4 ms → 0.97 ms），
因为堆版 `Vec<WaiterSlot>` 扩容是 O(N²) 复制，Arena 只是 chunk 线性分配。

### 6.2 deep-mixed（读/写 3:1 交错，每等待者领到后立即释放）

`per_waiter`（ns）：

| N | heap | arena | arena/heap |
|---:|---:|---:|---:|
| 1 | 690 | 825 | 1.20 |
| 16 | 528 | 429 | 0.81 |
| 128 | 1,317 | 1,125 | 0.85 |
| 1024 | 8,493 | 7,811 | 0.92 |
| 4096 | 32,352 | 30,188 | 0.93 |

读写混合下 Arena 只快 7～19%，远小于纯读者。因为每个写者放行都要单独一次
`pass`，且读者组只有 3 个，批量优势消失；两版都被“每个等待者完成即触发
一次放行”的 O(N²) 主导。

## 7. cancel-storm（K=512，取消 0/25/50/75/100%）

| 取消比例 | 指标 | heap (ns) | arena (ns) | arena/heap |
|---:|---|---:|---:|---:|
| 0% | drain | 762,986 | 83,146 | 0.11 |
| 0% | total | 769,794 | 103,150 | 0.13 |
| 25% | cancel | 65,904 | 9,385 | **0.14** |
| 25% | drain | 567,444 | 52,412 | 0.09 |
| 25% | total | 639,522 | 78,194 | **0.12** |
| 50% | cancel | 129,678 | 17,704 | **0.14** |
| 50% | drain | 382,076 | 34,888 | 0.09 |
| 50% | total | 514,930 | 66,232 | **0.13** |
| 75% | cancel | 199,058 | 26,314 | 0.13 |
| 75% | total | 395,528 | 60,134 | 0.15 |
| 100% | cancel | 265,628 | 42,882 | **0.16** |
| 100% | total | 266,466 | 43,766 | **0.16** |

- 取消是 Arena 最稳定的优势路径：`Waiting` 读者取消只做 CAS，不取队列锁；
  堆版每次取消都 `cancel_wait` 取锁 + `pass_` + `Vec<Waker>`。
- 剩余读者的 `drop` 依旧 Arena 慢 3～6.6 倍（回收批量节点），但绝对量只有
  10～20 µs，相对总耗时占比小。

## 8. upgrade-mixed（可升级读 → 升级 → 写者释放 → 回退 → 降级）

`upgrade` = 从发起 `upgrade_async` 到拿到写锁（ns）；`total` 含后续降级与
等待读者放行。每轮均验证了活性（所有等待者在超时前完成）：

| readers | heap upgrade | arena upgrade | arena/heap |
|---:|---:|---:|---:|
| 0 | 581 | 275 | 0.47 |
| 1 | 1,204 | 1,981 | 1.65 |
| 4 | 1,192 | 1,256 | 1.05 |
| 16 | 1,924 | 1,513 | 0.79 |

差异在 0.47～1.65× 之间且方向不一致，**无显著差异**；两版升级/回退路径都
正常，没有饥饿或死锁。

## 9. 分配计数（计数全局分配器 + 断言）

预热后，扣除测量装置（`Box<Session>`、`Box::pin(future)`）的逐桶差分：

| 场景 | heap 实现分配 | arena 实现分配 |
|---|---:|---:|
| steady（N=8，200 次完整流程） | 1,000 次 / 107,200 B | **0 次 / 0 B** |
| steady 每次流程 | 5 次 / 536 B | **0** |
| cold（100 把新锁） | 600 次 / 36,000 B | 300 次 / **6,567,200 B** |
| cold 每锁 | 6 次 / 360 B | 3 次 / 65,672 B（1 chunk） |
| deep N=4096 | 8,217 次 / 589,720 B | 8,198 次 / 491,616 B |

- **断言通过**：Arena steady 的逐桶差分全为 0；堆版必须 >0。
- 堆版 steady 的 5 次/流程分布：2×≤64B（Arc 节点 + `Vec<WaiterSlot>`）、
  2×≤128B（`Vec<Waker>` 等）、1×≤256B。
- Arena deep N=4096 的 >4096B 桶正好 **6 次**（5 个 chunk + 1 个其它大分配），
  其余 8,192 次全是测量装置的 32B Box；堆版实现分配 25 次。
- 本基准用 `Waker::noop()`，所以**没有运行时 Waker 分配**；“本 crate 分配”
  与“运行时分配”天然分离。

## 10. cold-start 与 memory

### 10.1 cold（每轮新建锁，写者+1 读者首次竞争，2000 次/轮）

| 指标 | heap | arena | arena/heap |
|---|---:|---:|---:|
| first_32 中位 | 609 | 6,984 | **11.5** |
| all 中位 | 608 | 6,784 | **11.2** |
| p99 | 613 | 7,230 | 11.8 |
| max | 11,404 | 106,428 | 9.3 |
| 100 锁后 RSS | 2,126 KB | 2,084 KB | 0.98 |

Arena 每把新锁首次入队要分配一个 65.6 KB 的 chunk（3 次分配），这是固定的
~6.2 µs 成本；堆版按节点分配，首次竞争只要 ~0.6 µs。RSS 反而不高，因为
分配器没有触碰 chunk 的全部页面。

### 10.2 memory（N=4096 深队列，3 次 burst，10 轮）

| 采样点 | heap (KB) | arena (KB) | arena/heap |
|---|---:|---:|---:|
| base | 2,532 | 2,300 | 0.91 |
| peak | 2,544 | 2,698 | 1.06 |
| VmHWM | 2,544 | 2,728 | 1.07 |
| drained | 2,566 | 2,762 | 1.08 |
| after release | 2,566 | 2,766 | 1.08 |
| after drop lock | 2,566 | 2,378 | 0.93 |

Arena 峰值高约 150～200 KB（≈3 个 chunk 的常驻部分），符合“保留一个空 tail
chunk”；锁销毁后反而低于堆版（堆版的节点/Vec 在分配器里留下更多碎片）。
差异在 6～8%，不构成内存问题。

## 11. 真实运行时并发（sanity）

tokio 固定 4 worker、compio 单线程、N=4 任务、每任务 5000 次读多写少 + 升级：

| 运行时 | heap (ns/op) | arena (ns/op) | arena/heap |
|---|---:|---:|---:|
| tokio 4 workers | 788 | 400 | **0.51** |
| compio 单线程 | 69 | 62 | 0.90 |

tokio 多线程下 Arena 快约 2 倍，和“读者领取/取消不取队列锁”的方向一致；
compio 单线程的 10% 差异接近噪声。OS 线程 `block_on` 一节按计划**未作为
判据**（调度噪声远大于实现差异）。

## 12. 与预测对照

| 计划中的预测 | 实测 | 判定 |
|---|---|---|
| fastpath 差异 ≤3% | 0～2.3% | ✅ 成立 |
| 竞争读者领取 Arena 占优 | N≥32 快 3～16× | ✅ 成立 |
| 读者释放 Arena 占优（只有最后一个触发 pass） | 单线程慢 1.5～3.3×；4 线程同时释放慢 1.55～2.6× | ❌ **不成立** |
| 大读者链 Arena 占优 | 总排空 N≥32 快 3～16×；但唤醒阶段 N≥128 慢 1.2～41.7× | ⚠️ 部分成立 |
| 深队列 Arena 占优 | 总耗时 N=4096 快 19× | ✅ 成立 |
| 取消风暴 Arena 占优 | 快 6～8× | ✅ 成立 |
| 升级混合接近 | 0.47～1.65×，方向不一致 | ✅ 视为无显著差异 |
| 稳态 Arena 0 分配（chunk 除外） | 0 次 / 0 B | ✅ 成立 |
| 稳态内存 Arena 略高 | 峰值 +6～8% | ✅ 成立 |
| 冷启动堆版更好 | Arena 慢 11.2× | ✅ 成立 |
| `WAKE_BATCH=32` 在大链上需要多轮取锁 | N=4096 release 慢 41.7×；调到 256 后降至 0.66 ms | ✅ 成立，且 32 偏小 |

## 13. 调参建议

### 13.1 `WAKE_BATCH`：32 偏小，建议 128（保守）或 256

实测（arena，reader-wake，10 轮中位数，`release` = 唤醒阶段）：

| WAKE_BATCH | N=512 | N=1024 | N=2048 | N=4096 |
|---:|---:|---:|---:|---:|
| 32 | 59.1 µs | 236 µs | 919 µs | 3.97 ms |
| 128 | 32.3 µs | 92.3 µs | 291 µs | 1.14 ms |
| 256 | 27.8 µs | 71.0 µs | 196 µs | 0.66 ms |

- 128 已带来 3.5× 改善，`WakerBatch` 只占 128×16B = 2 KB 栈；256 是 4 KB。
  两者都远小于默认 8 MB 栈。
- 但根因不是批大小，而是**每批都从 `RootRead.tail_` 重扫已放行节点**，
  复杂度 O(N²/B)。推荐在 `RootRead` 增加“当前放行游标”（或在 admit 后把
  `tail_`/游标推进到最后一个未放行节点），把总遍历降为 O(N)。只调
  `WAKE_BATCH` 是低风险缓解，游标才是根治。
- 建议先提交 `WAKE_BATCH = 128`（一行常量、可单独验证），再把游标作为
  后续优化单独评审。

### 13.2 `RootRead` 计数：不建议优先改

`inc_count_` / `dec_count_` 现在是 `AtomicFlags<usize>` 上的 CAS 循环。
无争用时 `lock cmpxchg` 与 `fetch_add/sub` 成本几乎相同；多线程下 CAS
伪失败/重试才会放大。实测 reader-release 的差距主要来自“两次原子 RMW +
批量 free”，而不是 CAS 重试。若要改，可用 `fetch_add(1)` / `fetch_sub(1)`
直接操作低位（不触碰 UPGR 位），但收益预计 <10%，优先级低于批次游标。

### 13.3 chunk 大小：建议 256（冷启动 5.9× 改善，深队列不变）

临时把 `CHUNK_CAPACITY` 从 1024 改成 256（5 轮中位数）：

| 指标 | CHUNK=1024 | CHUNK=256 | 变化 |
|---|---:|---:|---:|
| cold all_median | 6,813 ns | **1,146 ns** | **-83%** |
| cold first_32 | 6,994 ns | 1,166 ns | -83% |
| deep N=4096 enqueue | 987 µs | 991 µs | 持平 |
| deep N=4096 drain | 4.22 ms | 4.49 ms | 持平（噪声内） |
| reader-wake N=4096 drain | 4.83 ms | 4.85 ms | 持平 |

- 4096 节点需要 17 个 chunk（vs 5 个），但 chunk 分配次数增加的开销完全被
  “单次分配更小、页面触碰更少”抵消，深队列没有退化。
- 建议 `CHUNK_CAPACITY = 256`，并保留“空 tail chunk 保留、非 tail 空 chunk
  立即归还”的现状（memory 数据显示锁销毁后 Arena 反而更省）。

## 14. 噪声与可信度

- **小 N（≤64）的绝对差值在 1～20 µs，受进程内测量位置影响明显**：把
  `NS=32,31` 反转后结果跟着反转（见 §4），因此 N=31/33 的单点差异不作为
  设计判据；N≥128 的趋势在多轮、多配置下稳定复现。
- `schedutil` 使频率在 0.8～2.3 GHz 波动；已用绑核 + 10 轮交替 + 中位数
  缓解，但 p95/p99 仍会受偶发降频影响（例如 cold 的 max 106 µs）。
- 4 核 Celeron 上 4 个 worker + 主线程的并发释放场景会引入 ~10 µs 级调度
  延迟，因此只看 per-thread `drop` 区间，不看总 wall time。
- 所有“实现分配”都做了逐桶扣除测量装置分配；Arena steady=0 的断言在 3 轮
  重复中完全一致，可信。
- OS 线程 `block_on` 混合负载一节未纳入判据，符合计划。

### 14.1 关键场景 p95/p99 附录

10 轮样本按 nearest-rank 取 p95/p99 时，两个分位都落在最后 1～2 个样本上，
因此下表读作“接近最慢轮的耗时”，用来判断尾部是否失控；完整逐桶数据在
`.tmp/bench-simp-read/summary.csv`。

| 场景 | 配置 | 指标 | heap 中位 / p95 / p99 | arena 中位 / p95 / p99 |
|---|---|---|---|---|
| reader-wake | 32 | drain/waiter | 504 / 548 / 548 | 162 / 163 / 163 |
| reader-wake | 128 | drain/waiter | 603 / 725 / 725 | 185 / 201 / 201 |
| reader-wake | 1024 | drain/waiter | 3,545 / 3,647 / 3,647 | 356 / 531 / 531 |
| reader-wake | 4096 | drain/waiter | 18,788 / 19,103 / 19,103 | 1,195 / 1,221 / 1,221 |
| reader-wake | 1024 | release | 24,994 / 28,762 / 28,762 | 236,729 / 242,118 / 242,118 |
| reader-wake | 4096 | release | 105,328 / 115,986 / 115,986 | 4,392,802 / 4,493,142 / 4,493,142 |
| reader-release | 32 | release_all | 550 / 573 / 573 | 1,172 / 1,195 / 1,195 |
| reader-release | 64 | release_all | 978 / 1,241 / 1,241 | 2,104 / 2,126 / 2,126 |
| cancel | 50% | total | 514,930 / 527,636 / 527,636 | 66,232 / 82,554 / 82,554 |
| cancel | 100% | total | 266,466 / 279,564 / 279,564 | 43,766 / 57,951 / 57,951 |
| deep | 1024 | total | 4,611,518 / 4,634,480 / 4,634,480 | 559,910 / 585,411 / 585,411 |
| deep | 4096 | total | 101,622,954 / 102,287,343 / 102,287,343 | 5,452,657 / 5,545,341 / 5,545,341 |
| cold | 每锁 | all | 608 / 616 / 616 | 6,784 / 7,029 / 7,029 |
| concurrent | 4 | tokio ns/op | 788 / 1,113 / 1,113 | 400 / 588 / 588 |
| concurrent | 4 | compio ns/op | 69 / 82 / 82 | 62 / 67 / 67 |

## 15. 下一步

1. **（高优先）** 把 `WAKE_BATCH` 调到 128，并实现读者链放行游标，消除
   `release` 阶段的 O(N²/B)；目标：N=4096 release 进入 100～200 µs 量级。
2. **（高优先）** 评估 reader-release：如果真实负载以“读者逐个释放”为主，
   需要权衡 Arena 的批量回收；可考虑延迟回收（把空组挂到待回收链，释放
   路径只 dec_count，下一次 pass 批量归还），或把 `active_group_` 读取与
   计数合并成一次原子。
3. **（中优先）** `CHUNK_CAPACITY = 256`，并补一个“chunk 增长/归还”的
   分配计数断言。
4. **（中优先）** 补 tokio 固定 worker 下的 reader-release / cancel 并发
   场景，验证多线程争用下 §5 的结论是否仍成立。
5. 本次未改核心代码，未重跑 Miri；若第 1/2 项落地，需按计划跑
   `MIRIFLAGS='-Zmiri-strict-provenance' cargo miri test -p atomic_sync
   rwlock::cooperative_arena::core_::tests_` 与全量测试。
