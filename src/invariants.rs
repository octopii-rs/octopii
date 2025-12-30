#[cfg(feature = "simulation")]
pub(crate) fn sim_assert(condition: bool, message: &str) {
    if !condition {
        panic!("simulation invariant failed: {message}");
    }
}

#[cfg(not(feature = "simulation"))]
#[inline]
pub(crate) fn sim_assert(_condition: bool, _message: &str) {}
