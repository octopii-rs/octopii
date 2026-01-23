pub struct SimRng {
    state: u64,
}

impl SimRng {
    pub fn new(seed: u64) -> Self {
        let mut rng = Self { state: 0 };
        rng.state = seed.wrapping_add(0x9E3779B97F4A7C15);
        rng.next();
        rng
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    pub fn range(&mut self, min: usize, max: usize) -> usize {
        let range = max - min;
        if range == 0 {
            return min;
        }
        min + (self.next() as usize % range)
    }

    #[allow(dead_code)]
    pub fn bool(&mut self, probability: f64) -> bool {
        let limit = (u64::MAX as f64 * probability) as u64;
        self.next() < limit
    }

    pub fn gen_payload(&mut self) -> Vec<u8> {
        let len = self.range(1, 21);
        let mut buf = Vec::with_capacity(len);
        for _ in 0..len {
            buf.push(self.next() as u8);
        }
        buf
    }
}
