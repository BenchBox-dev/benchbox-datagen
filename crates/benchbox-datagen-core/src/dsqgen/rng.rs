const MODULUS: u64 = 2_147_483_647;
const MULTIPLIER: u64 = 16_807;
const GROUP_ORDER: u64 = MODULUS - 1;
const ORDER_FACTORS: [(u64, u32); 7] =
    [(2, 1), (3, 2), (7, 1), (11, 1), (31, 1), (151, 1), (331, 1)];
const DENSE_LIMIT: u64 = 1 << 20;

fn mul_mod(left: u64, right: u64) -> u64 {
    (left * right) % MODULUS
}

fn pow_mod(mut base: u64, mut exponent: u64) -> u64 {
    let mut result = 1;
    base %= MODULUS;
    while exponent > 0 {
        if exponent & 1 == 1 {
            result = mul_mod(result, base);
        }
        base = mul_mod(base, base);
        exponent >>= 1;
    }
    result
}

fn discrete_log(target: u64) -> u64 {
    let mut residue = 0_u128;
    let mut modulus = 1_u128;
    for (prime, power) in ORDER_FACTORS {
        let order = prime.pow(power);
        let generator = pow_mod(MULTIPLIER, GROUP_ORDER / order);
        let wanted = pow_mod(target, GROUP_ORDER / order);
        let mut current = 1;
        let mut exponent = 0_u64;
        while current != wanted {
            current = mul_mod(current, generator);
            exponent += 1;
        }
        let order = u128::from(order);
        let mut step = 0;
        while (residue + modulus * step) % order != u128::from(exponent) {
            step += 1;
        }
        residue += modulus * step;
        modulus *= order;
    }
    residue as u64
}

#[derive(Debug, Clone)]
pub(crate) struct Rng {
    state: u64,
}

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub(crate) fn state(&self) -> u64 {
        self.state
    }

    pub(crate) fn reset(&mut self, seed: u64) {
        self.state = seed;
    }

    pub(crate) fn next(&mut self) -> u64 {
        self.state = mul_mod(self.state, MULTIPLIER);
        self.state
    }

    pub(crate) fn skip(&mut self, count: u64) {
        self.state = mul_mod(self.state, pow_mod(MULTIPLIER, count));
    }

    pub(crate) fn integer(&mut self, min: i64, max: i64) -> i64 {
        let span = max - min + 1;
        (self.next() as i64) % span + min
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Pass {
    start: u64,
    inverse_start: u64,
}

#[derive(Debug, Clone)]
pub(crate) enum KeyPermutation {
    Dense { values: Vec<u32>, shuffled: u64 },
    Lazy { size: u64, passes: Vec<Pass> },
}

impl KeyPermutation {
    pub(crate) fn new(size: u64) -> Self {
        if size <= DENSE_LIMIT {
            Self::Dense {
                values: (0..size as u32).collect(),
                shuffled: 0,
            }
        } else {
            Self::Lazy {
                size,
                passes: Vec::new(),
            }
        }
    }

    pub(crate) fn size(&self) -> u64 {
        match self {
            Self::Dense { values, .. } => values.len() as u64,
            Self::Lazy { size, .. } => *size,
        }
    }

    pub(crate) fn shuffles(&self) -> u64 {
        match self {
            Self::Dense { shuffled, .. } => *shuffled,
            Self::Lazy { passes, .. } => passes.len() as u64,
        }
    }

    pub(crate) fn shuffle(&mut self, rng: &mut Rng) {
        match self {
            Self::Dense { values, shuffled } => {
                *shuffled += 1;
                let size = values.len() as i64;
                for index in 0..values.len() {
                    let other = rng.integer(0, size - 1) as usize;
                    values.swap(index, other);
                }
            }
            Self::Lazy { size, passes } => {
                let start = rng.state();
                passes.push(Pass {
                    start,
                    inverse_start: if start == 0 {
                        0
                    } else {
                        pow_mod(start, MODULUS - 2)
                    },
                });
                rng.skip(*size);
            }
        }
    }

    pub(crate) fn entry(&self, index: u64) -> Option<u64> {
        match self {
            Self::Dense { values, .. } => values.get(index as usize).map(|value| u64::from(*value)),
            Self::Lazy { size, passes } => {
                if index >= *size {
                    return None;
                }
                let mut position = index;
                for pass in passes.iter().rev() {
                    position = trace_pass(pass, *size, position);
                }
                Some(position)
            }
        }
    }
}

fn draw_after(pass: &Pass, steps: u64) -> u64 {
    mul_mod(pass.start, pow_mod(MULTIPLIER, steps))
}

fn trace_pass(pass: &Pass, size: u64, mut position: u64) -> u64 {
    let mut limit = size;
    loop {
        let mut event: Option<u64> = None;
        if position < limit {
            event = Some(position);
        }
        if pass.start == 0 {
            if position == 0 && limit > 0 && event.is_none_or(|found| limit - 1 > found) {
                event = Some(limit - 1);
            }
        } else {
            let mut candidate = position;
            while candidate < MODULUS {
                if candidate != 0 {
                    let exponent = discrete_log(mul_mod(candidate, pass.inverse_start));
                    if exponent >= 1 && exponent <= limit {
                        let step = exponent - 1;
                        if event.is_none_or(|found| step > found) {
                            event = Some(step);
                        }
                    }
                }
                candidate += size;
            }
        }
        let Some(step) = event else {
            return position;
        };
        let partner = draw_after(pass, step + 1) % size;
        if step == position {
            position = partner;
        } else if partner == position {
            position = step;
        }
        limit = step;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discrete_log_inverts_the_multiplier_power() {
        for exponent in [1_u64, 2, 17, 1_000, 123_456_789, 2_147_483_645] {
            assert_eq!(discrete_log(pow_mod(MULTIPLIER, exponent)), exponent);
        }
    }

    #[test]
    fn lazy_permutations_equal_dense_shuffles() {
        for (seed, size, passes) in [
            (1_u64, 1_200_000_u64, 1),
            (42, 1_500_000, 3),
            (1_339_962_585, 1_100_000, 2),
            (0, 1_100_000, 2),
        ] {
            let mut dense_rng = Rng::new(seed);
            let mut dense = KeyPermutation::Dense {
                values: (0..size as u32).collect(),
                shuffled: 0,
            };
            let mut lazy_rng = Rng::new(seed);
            let mut lazy = KeyPermutation::Lazy {
                size,
                passes: Vec::new(),
            };
            for _ in 0..passes {
                dense.shuffle(&mut dense_rng);
                lazy.shuffle(&mut lazy_rng);
            }
            assert_eq!(dense_rng.state(), lazy_rng.state());
            for index in (0..8).chain([size / 2, size - 1]) {
                assert_eq!(
                    dense.entry(index),
                    lazy.entry(index),
                    "seed {seed} index {index}"
                );
            }
        }
    }
}
