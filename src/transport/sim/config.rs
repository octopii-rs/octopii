#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub drop_rate: f64,
    pub min_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: 1,
            drop_rate: 0.0,
            min_delay_ms: 0,
            max_delay_ms: 0,
        }
    }
}
