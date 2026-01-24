#[derive(Clone)]
pub struct SimRng {
    state: u64,
}

const GOLDEN_RATIO: u64 = 0x9E3779B97F4A7C15;

impl SimRng {
    pub fn new(seed: u64) -> Self {
        let mut rng = Self { state: 0 };
        rng.state = seed.wrapping_add(GOLDEN_RATIO);
        rng.next_u64();
        rng
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    pub fn next_range(&mut self, upper_exclusive: u64) -> u64 {
        if upper_exclusive <= 1 {
            return 0;
        }
        self.next_u64() % upper_exclusive
    }

    pub fn range(&mut self, min: usize, max: usize) -> usize {
        let range = max - min;
        if range == 0 {
            return min;
        }
        min + (self.next_u64() as usize % range)
    }

    pub fn gen_payload(&mut self) -> Vec<u8> {
        let len = self.range(1, 21);
        let mut buf = Vec::with_capacity(len);
        for _ in 0..len {
            buf.push(self.next_u64() as u8);
        }
        buf
    }
}
