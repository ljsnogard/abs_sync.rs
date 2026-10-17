pub mod cooperative;
pub mod preemptive;

pub(super) trait TrShareMut<'a, T: ?Sized> {
    fn share_mut(&mut self) -> &'a mut T;
}
