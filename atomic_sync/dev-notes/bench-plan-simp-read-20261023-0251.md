# 两套 cooperative 读写锁的差异测试计划（feat/simp-read）

基线提交：`6d7aca5`（`feat/simp-read`）
日期：2026-10-23
目标：先给出一个可复现的冒烟基线，再明确**简化后的 Arena 版**与**堆分配版**
最可能出现性能差异的路径，以及下一会话如何用测试把这些差异暴露出来。

## 1. 冒烟基线

### 1.1 环境与命令

- CPU：Intel(R) Celeron(R) CPU J3455 @ 1.50GHz，4 线程
- 工具链：`rustc 1.101.0-nightly (4ebd25825 2026-10-15)`
- 未做 CPU 绑核、未固定频率；每项取 3 轮最好成绩
- 规模：每个竞争者 5000 次操作，临界区自旋 8，竞争者 `1,2,4`
- 负载：
  - readheavy：读 75% / 写 15% / 升级 5% / 让出 5%
  - writeheavy：读 40% / 写 40% / 升级 15% / 让出 5%

```sh
RWLOCK_BENCH_OPS=5000 RWLOCK_BENCH_ROUNDS=3 \
RWLOCK_BENCH_COMPETITORS=1,2,4 RWLOCK_BENCH_HOLD=8 \
cargo bench -p atomic_sync --bench rwlock_cooperative
```

两套实现通过**临时修改 bench 的 import** 分别测量：

```diff
- cooperative::CooperativeRwLockOwned,
+ cooperative_arena::CooperativeRwLockOwned,
```

跑完后 `git checkout -- atomic_sync/benches/rwlock_cooperative.rs` 还原。当前
bench 还没有“运行时选择实现”的开关，这是下一会话要补的测试设施（见 §4）。
原始输出保存在工作区 `.tmp/bench_smoke3_heap.txt` 与
`.tmp/bench_smoke3_arena.txt`（不随仓库提交）。

### 1.2 结果（ns/op，越小越好）

readheavy：

| 运行时 | N | 堆分配版 | Arena 版 |
|---|---:|---:|---:|
| tokio | 1 | 95.5 | 79.8 |
| tokio | 2 | 186.1 | 130.4 |
| tokio | 4 | 62.8 | 95.0 |
| compio | 1 | 87.7 | 81.2 |
| compio | 2 | 60.6 | 56.5 |
| compio | 4 | 61.6 | 54.4 |
| OS 线程 | 1 | 68.5 | 82.1 |
| OS 线程 | 2 | 368.3 | 1577.2 |
| OS 线程 | 4 | 3696.9 | 3645.1 |

writeheavy：

| 运行时 | N | 堆分配版 | Arena 版 |
|---|---:|---:|---:|
| tokio | 1 | 80.3 | 74.6 |
| tokio | 2 | 287.0 | 226.3 |
| tokio | 4 | 172.3 | 251.0 |
| compio | 1 | 95.6 | 87.1 |
| compio | 2 | 64.1 | 59.5 |
| compio | 4 | 66.8 | 61.7 |
| OS 线程 | 1 | 84.8 | 82.9 |
| OS 线程 | 2 | 6372.0 | 716.2 |
| OS 线程 | 4 | 8185.6 | 8153.8 |

### 1.3 如何解读

- **N=1 两版接近**：快速路径使用的是同一套状态字逻辑，差异应主要来自代码
  布局/测量噪声。Arena 版在 N=1 的 5%～9% 优势不足以作为结论。
- **compio 单线程最稳定**：readheavy N=2/4、writeheavy N=2/4 上 Arena 版大约
  快 5%～12%。这与“读者领取/取消/释放移出队列锁”的预期方向一致。
- **tokio 多线程方差很大**：readheavy N=4 Arena 反而更慢、writeheavy N=4
  Arena 又更慢，和 N=2 的方向不一致。当前样本量不足以判断。
- **OS 线程一节的 `cooperative` 用 `block_on` 驱动**，N≥2 时被调度噪声完全
  主导（同一实现 N=2 与 N=4 能差一个数量级），不应作为两版差异的依据。
- 这次冒烟**没有覆盖**：大读者链、深队列、取消风暴、冷启动、分配计数、
  并发释放、RSS。因此它只能作为“有没有明显异常”的冒烟基线，不能作为
  简化设计性能优劣的结论。

## 2. 预期差异与原因

| 路径 | 堆分配版 | Arena 简化版 | 预期方向 | 如何暴露 |
|---|---|---|---|---|
| 快速路径（无竞争） | 状态字 CAS | 同一套状态字 CAS | 基本无差异（≤3%） | 单线程 `try_read/try_write` 循环，N=1 |
| 竞争读者领取 | `poll_acquire` 取队列锁 | `AtomicFlags` 状态机 + 全局 CAS，无队列锁 | Arena 在高并发读者下更好 | 写者持锁时排队 N 个读者，释放后计时“唤醒→领取→排空” |
| 读者释放 | 每次 `release_reader` 都 `run_pass_`（取队列锁） | 原子计数，只有最后一个触发 pass | Arena 在多读者并发释放时更好 | N 个读者领取后 barrier，同时释放，测总耗时 |
| 读者链唤醒 | 读类节点 `Vec<WaiterSlot>`，一次收集 waker | LIFO 读者链，每批 32 个 waker | 大链 Arena 更好；>32 时 Arena 需要多轮取锁 | 读者数 N=1..4096，观察 32/1024 边界 |
| 深队列 | `VecDeque` + 每节点 `Arc`/`Vec` | 1024 槽 chunk，无逐节点堆分配 | 深队列 Arena 更好；冷启动/小队列堆版更好 | 队列深度扫描 + 分配计数 + RSS |
| 取消风暴 | 取队列锁、槽位标取消 | 原子状态 + 计数，wake 跳过 Cancelled | 高取消比例 Arena 更好 | 排队 K 个，取消 0/25/50/75/100% 后释放 |
| 升级混合 | 升级插队首，读类节点槽位扫描 | 升级节点插组后继，`UPGR` 位 | 接近；升级+读者竞争下 Arena 可能更好 | 可升级读 + 普通读 + 写者混合负载，测升级延迟/吞吐 |
| 分配 | 每等待者 `Arc` + `Vec` | 稳态只分配 chunk | Arena 稳态 0 次本 crate 分配 | 计数全局分配器，预热后归零再跑 K 次 |
| 稳态内存 | 排空后基本归还 | 保留一个空 tail chunk | Arena 稳态 RSS 略高，但无 per-op 分配 | 深队列峰值后排空，采样 RSS/VmHWM |

几个具体判断：

1. **Arena 应该赢的地方**：大读者链、深队列、取消风暴、多线程并发读者释放/
   领取。原因是这些路径上堆分配版仍有队列锁和逐节点堆分配，而 Arena 版把
   读者领取/取消/注册移到了原子状态机，并且回收是整链批量。
2. **堆分配版可能赢的地方**：冷启动第一次竞争（Arena 一次申请 1024 槽
   chunk）、小队列（Arena 的 chunk 初始化/内存占用更大）、以及可能存在的
   `AtomicFlags<usize>` CAS 计数开销（堆版用状态字 CAS + fetch 语义）。
3. **没有把握的地方**：tokio 多线程、OS 线程。调度噪声远大于实现差异，必须
   绑核、固定 worker、增加轮数并取中位数。

## 3. 测试场景

### A. fastpath（对照）

- 单线程，无竞争，循环 `try_read`/`try_write`。
- 指标：ns/op。
- 预期：两版差异 ≤3%；若超过，先查代码布局/测量噪声，不急着归因设计。

### B. reader-wake（重点）

- 主任务持有写锁；spawn N 个读者排队（同一读者组）；释放写者；等待全部
  完成。
- N 扫描：`1,2,4,8,16,31,32,33,64,128,512,1024,2048,4096`。
- 指标：入队耗时、唤醒到全部领取的耗时、每读者 ns、p99、分配次数。
- 预期：N≥64 后 Arena 占优；重点看 N=31/32/33（批次边界）和 N=1024/2048
  （chunk/VecDeque 边界）。如果 Arena 在 33 附近明显变差，考虑调大
  `WAKE_BATCH` 或改成分批收集。

### C. reader-release（重点）

- 先让 N 个读者进入同一组并领取（写者先持有再释放）；用 barrier 让它们
  **同时释放**。
- N：`2,4,8,16,32,64`。
- 指标：全部释放完成耗时、每读者 ns、队列锁获取次数。
- 预期：堆版每次释放都进 `run_pass_`，Arena 只有最后一个进；N 越大差距
  应越明显。这是本次简化最应该体现优势的场景之一。

### D. deep-queue

- 写者持有；排队 N 个等待者（读者/写者混合）；释放写者并 drain。
- N 扫描到 4096；记录每等待者 ns、峰值 RSS、chunk 数/分配次数。
- 预期：N>1024 后 Arena 更稳；堆版在 `VecDeque` 扩容点可能出现阶跃；
  Arena 冷启动/小 N 可能略慢。

### E. cancel-storm

- 排队 K 个等待者，取消比例 `0/25/50/75/100%`，再释放写者。
- 指标：取消耗时、drain 耗时、分配/回收次数、残留队列。
- 预期：高取消比例下 Arena 的原子取消 + 跳过 Cancelled 更好；低取消比例、
  小队列时堆版可能接近或更好。

### F. upgrade-mixed

- 一个可升级读 + 若干普通读 + 写者混合；包含“升级 → 写者释放 → 回退 →
  降级为普通读”。
- 指标：升级等待时间、整体吞吐、是否有饥饿/死锁。
- 预期：接近；Arena 的“升级节点插组后继”在升级+读者竞争下可能更好。
  这更多是正确性/活性测试，性能差异不是主要目标。

### G. cold-start

- 反复创建新锁：先取写者，再让 1 个等待者入队并释放；重复很多次。
- 指标：首次入队时延、分配次数、RSS 增量。
- 预期：Arena 因 1024 槽 chunk 初始化而更慢、RSS 更高；堆版逐节点分配
  在小规模下更轻。这个场景用于确认“Arena 不是所有维度都更快”。

### H. allocation-count

- 专用测试二进制 + 计数型全局分配器。
- 预热到 chunk 已分配、队列稳态后清零计数；执行 K 次“入队 → 唤醒 → 领取 →
  释放”。
- 断言：Arena 除 chunk 外 0 次分配；堆版 >0。按分配大小分类，排除运行时
  `Waker` 自身的分配。

### I. memory/RSS

- 深队列峰值后排空，记录 RSS/`VmHWM`；重复“突发-排空-再突发”。
- 预期：Arena 保留一个空 tail chunk，稳态内存略高；堆版排空后更接近基线。

## 4. 测试设施（下一会话先补）

当前 bench 只能通过改 import 切换实现，不适合长期对比。建议二选一：

1. **推荐：两个 bench target 共用一份源**
   - 把 bench 主体抽到 `rwlock_common.rs`，用 `type TestLock = ...` 参数化；
   - `benches/rwlock_heap.rs` / `benches/rwlock_arena.rs` 各自定义
     `TestLock` 后 `include!` 主体；
   - 两个 target 用相同的 `RWLOCK_BENCH_*` 环境和参数，交替运行。
2. **临时方案：脚本切换 import**
   - 在 `.tmp` 里复制 bench、`sed` 替换 import、运行、还原；
   - 只用于过渡，不提交重复源文件。

另外需要补：

- `RWLOCK_BENCH_SCENARIO=fastpath|reader-wake|reader-release|deep|cancel|upgrade|alloc|memory`
- `RWLOCK_BENCH_WAITERS`、`RWLOCK_BENCH_CANCEL_PCT`、`RWLOCK_BENCH_IMPL`
- 统计：至少 10 轮，报告中位数、p95、p99，不只取最好轮；两版交替运行。
- 环境：`taskset` 绑核、记录 cpufreq、同一 release/bench profile、同一随机种子。
- 分配计数与 RSS 采集作为独立测试二进制，避免污染主 bench。

## 5. 下一会话执行清单

- [ ] 完成双实现 bench target（或临时脚本）并验证两边输出列名正确。
- [ ] 实现场景 A～F、H、I；先跑 `OPS=2000` 的冒烟确认无挂起。
- [ ] 绑核 + 10 轮 + 中位数/p99，跑 N 扫描和取消比例扫描。
- [ ] 记录两版在 reader-wake / reader-release / deep / cancel 上的差异。
- [ ] 根据结果决定是否调参：
  - `WAKE_BATCH`（32 是否偏小）；
  - `RootRead` 计数是否从 `AtomicFlags<usize>` CAS 改回 `fetch_add/sub`
    （代价是回到两个原子或自定义组合原子）；
  - Arena chunk 容量/空 chunk 保留策略（冷启动 vs 稳态内存）。
- [ ] 跑现有测试 + Miri，确认优化没有破坏正确性。

### 判定阈值（初步）

- fastpath 回退 ≤3%；
- reader-wake / reader-release / cancel 在 N≥64 或取消≥50% 时，Arena 相对
  堆版提升 ≥10%，或 p99 改善 ≥15%；
- 深队列 N≥1024 时 Arena 不出现比堆版更差的阶跃；
- 稳态等待路径 Arena 的本 crate 分配为 0（chunk 除外）；
- 若只有 N=1 或小队列有优势、重点场景没有优势，则说明简化主要改善了代码
  结构而不是性能，需要重新评估是否保留某些原子化/批量策略。

## 6. 已知噪声与风险

- 这台 Celeron J3455 + 4 线程的调度噪声很大；tokio 多线程 N=4 的单轮结果
  不可信，必须绑核 + 多轮中位数。
- OS 线程一节 `cooperative` 用 `block_on` 驱动，和原生阻塞/自旋不是同类
  对照；只作 sanity check，不作为两版差异判据。
- `Waker::clone`/`wake` 的运行时分配可能污染分配计数，需要按大小分类并
  区分“本 crate 分配”和“运行时分配”。
- 仅改 import 会改变代码布局，可能带来少量 ICache/布局噪声；双 target 共用
  源可以减少这种差异。
- 当前冒烟每项只有 3 轮最好成绩，不能用来下“谁更快”的最终结论。
