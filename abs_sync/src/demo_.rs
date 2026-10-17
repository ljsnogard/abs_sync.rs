use std::{
    borrow::BorrowMut,
    ops::{Deref, DerefMut},
};

use abs_cancel::{TrCancellationToken, TrMayCancel};

use crate::async_rwlock::*;

#[allow(dead_code)]
async fn generic_rwlock_smoke_<B, L, T, K>(
    rwlock: B,
    cancel: K,
)
where
    B: BorrowMut<L>,
    L: TrAsyncRwLock<Target = T>,
    K: TrCancellationToken,
{
    let mut acq_sess = rwlock.borrow().acq_session();

    // let read_guard = read_async
    //     .may_cancel_with(&mut NonCancellableToken::new())
    //     .await?;
    let read_guard = acq_sess
        .read_async()
        .may_cancel_with(cancel.child_token())
        .await
        .expect("");
    // the following line will be illegal, which
    // let write_async = rwlock.borrow().session().write_async();

    let _ = read_guard.deref();
    drop(read_guard);

    let upgradable = acq_sess
        .upgradable_read_async()
        .may_cancel_with(cancel.child_token())
        .await
        .unwrap();
    let _ = upgradable.deref();

    let mut upg_sess = upgradable.upgrade_session();
    let mut write_guard = upg_sess
        .upgrade_async()
        .may_cancel_with(cancel.child_token())
        .await
        .unwrap();
    let _ = write_guard.deref_mut();

    let upgradable = write_guard.downgrade_to_upgradable();
    drop(upgradable);
}
