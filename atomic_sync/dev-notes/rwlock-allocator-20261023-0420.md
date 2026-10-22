# 两套 CooperativeRwLock 增加分配器参数

分支：`feat/simp-read`
基线：`56f8b56`（WAKING 独占读者链）
日期：2026-10-23

## 1. 目标

给两套 `CooperativeRwLock` 的泛型参数表最右边增加一个分配器参数 `A`，
默认值为全局分配器 `alloc::alloc::Global`。要求该参数**真正用于等待队列
分配**，而不是仅做类型占位。

## 2. 公开 API

两套锁的形状现在一致：

```rust
pub struct CooperativeRwLock<
    T: ?Sized,
    D = usize,
    B = AtomicUsize,
    O = StrictOrderings,
    A: Allocator = Global,
> { ... }
```

类型别名：

```rust
pub type CooperativeRwLockOwned<T, C = AtomicUsize, O = StrictOrderings, A = Global> =
    CooperativeRwLock<T, <C as TrAtomicCell>::Value, C, O, A>;

pub type CooperativeRwLockBorrowed<'a, T, C = AtomicUsize, O = StrictOrderings, A = Global> =
    CooperativeRwLock<T, <C as TrAtomicCell>::Value, &'a mut C, O, A>;
```

构造入口：

- `new(data, cell)` / `new_owned(data)`：使用 `A::default()`（默认 `Global`）；
- `new_in(data, cell, alloc)` / `new_owned_in(data, alloc)`：传入有状态分配器。

`A` 的约束是 `core::alloc::AllocatorClone`（`Allocator + Clone` 的 unsafe
标记 trait）。这是因为 `Arc<T, A>` 的 `Clone` 实现要求 `AllocatorClone`；
`Global` 已实现该 trait。自定义分配器若要与 `Arc` 一起使用，需要按标准库
文档实现 `unsafe impl AllocatorClone`。

会话、Guard 与 Future 的公共类型也增加了 `A`，但都设了 `A = Global`
默认值，因此现有只写 `ReaderGuard<'a, 'g, T, D, B, O>` 的代码仍然成立。

`allocator_ext` 特性已加入 crate 根：

```rust
#![feature(allocator_ext)]
```

它是 `Arc`/`Vec`/`VecDeque` 的 allocator API 所需的 nightly 特性；1.101
nightly 上 `allocator_api` 已部分稳定，使用 `allocator_ext` 不再产生
`stable_features` 警告。

## 3. 堆分配版（cooperative）

分配器沿等待路径完整传播：

| 位置 | 类型 |
|---|---|
| 队列 | `VecDeque<Arc<WaitNode<A>, A>, A>` |
| 节点外壳 | `Arc<WaitNode<A>, A>` |
| 节点槽位 | `WaitNode<A> { slots_: SpinLock<Vec<WaiterSlot, A>> }` |
| 放行临时 waker | `Vec<Waker, A>` |
| 共享内核外壳 | `Arc<RwCore<D, B, O, A>, A>` |

`RwCore` 持有 `alloc_: A`，入队时用 `self.alloc_.clone()` 创建节点与 `Arc`；
`pass_` 用 `Vec::new_in(self.alloc_.clone())` 收集 waker。

## 4. Arena 版（cooperative_arena）

| 位置 | 类型 |
|---|---|
| 节点池 | `Arena<WaitNode, A>`（chunk 走 `A`） |
| 队列载荷 | `QueuePayload<A> { arena_: Arena<WaitNode, A>, ... }` |
| 共享内核外壳 | `Arc<RwCoreArena<D, B, O, A>, A>` |

Arena 版没有逐节点分配，`A` 主要用于 chunk 分配与 `Arc` 外壳。

## 5. 验证

- `cargo test -p atomic_sync`：
  88 单测 + 2 个 cooperative_arena 集成 + 7 个 cooperative 集成
  + 2 个自定义分配器集成 + 8 个 preemptive + 4 个文档测试全部通过。
- `cargo clippy -p atomic_sync --all-targets`：无告警。
- 新增 `tests/custom_allocator.rs`：
  - `CountingAlloc` 把 `allocate` 转发给 `Global` 并计数；
  - 两套锁分别用 `new_in(..., CountingAlloc)` 构造，排队一个读者后断言
    分配计数严格增长，证明等待队列/chunk 没有绕过 `A`。
- 分配计数回归：默认 `Global` 下堆版仍是 5 次/流程、Arena 仍是 0 次实现
  分配；`fastpath` 仍为 ~21–22 ns/op。

## 6. 兼容性与剩余事项

- 默认参数 `A = Global` 保证现有 `CooperativeRwLockOwned<T>` 调用不需要改。
- `new`/`new_owned` 要求 `A: Default`；有状态分配器用 `new_in`/
  `new_owned_in`，或为分配器实现 `Default`。
- `Global` 本身是 nightly allocator API 的类型；调用方若要显式写出
  `Global` 类型，需要启用 `allocator_ext`。只用默认参数时不需要。
- 锁算法（WAKING、快速路径、取消、升级）没有任何逻辑改动，仅替换了
  分配来源。
