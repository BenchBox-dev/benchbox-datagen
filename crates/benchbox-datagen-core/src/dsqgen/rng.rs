use std::sync::OnceLock;

const MODULUS: u64 = 2_147_483_647;
const MULTIPLIER: u64 = 16_807;
const GROUP_ORDER: u64 = MODULUS - 1;
const ORDER_FACTORS: [(u64, u32); 7] =
    [(2, 1), (3, 2), (7, 1), (11, 1), (31, 1), (151, 1), (331, 1)];
const DENSE_LIMIT: u64 = 1 << 24;
const LAZY_MIN_MODULUS: u64 = 1 << 16;

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

struct Factor {
    cofactor: u64,
    order: u64,
    inverse_of_prior: u64,
    powers: Vec<(u64, u64)>,
}

fn factors() -> &'static [Factor] {
    static FACTORS: OnceLock<Vec<Factor>> = OnceLock::new();
    FACTORS.get_or_init(|| {
        let mut prior = 1_u64;
        let mut built = Vec::new();
        for (prime, power) in ORDER_FACTORS {
            let order = prime.pow(power);
            let cofactor = GROUP_ORDER / order;
            let generator = pow_mod(MULTIPLIER, cofactor);
            let mut powers = Vec::new();
            let mut current = 1;
            for exponent in 0..order {
                powers.push((current, exponent));
                current = mul_mod(current, generator);
            }
            powers.sort_unstable();
            let inverse_of_prior = (1..order)
                .find(|candidate| (prior % order) * candidate % order == 1)
                .unwrap_or(0);
            built.push(Factor {
                cofactor,
                order,
                inverse_of_prior,
                powers,
            });
            prior *= order;
        }
        built
    })
}

fn discrete_log(target: u64) -> Option<u64> {
    let mut residue = 0_u64;
    let mut modulus = 1_u64;
    for factor in factors() {
        let wanted = pow_mod(target, factor.cofactor);
        let at = factor
            .powers
            .binary_search_by_key(&wanted, |(power, _)| *power)
            .ok()?;
        let exponent = factor.powers[at].1;
        let gap = (exponent + factor.order - residue % factor.order) % factor.order;
        residue += modulus * (gap * factor.inverse_of_prior % factor.order);
        modulus *= factor.order;
    }
    Some(residue)
}

pub(crate) fn key_modulus(size: u64) -> Option<u64> {
    match size as i32 {
        0 => None,
        truncated => Some(u64::from(truncated.unsigned_abs())),
    }
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

    pub(crate) fn integer(&mut self, min: i64, span: i64) -> i64 {
        (self.next() as i64).wrapping_rem(span).wrapping_add(min)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Pass {
    start: u64,
    inverse_start: u64,
}

#[derive(Debug, Clone)]
pub(crate) enum KeyPermutation {
    Dense {
        values: Vec<u32>,
        modulus: u64,
    },
    Lazy {
        size: u64,
        modulus: u64,
        passes: Vec<Pass>,
    },
}

impl KeyPermutation {
    pub(crate) fn new(size: u64) -> Option<Self> {
        let modulus = key_modulus(size)?;
        if size > DENSE_LIMIT && modulus < LAZY_MIN_MODULUS {
            return None;
        }
        Some(if size <= DENSE_LIMIT {
            Self::Dense {
                values: (0..size as u32).collect(),
                modulus,
            }
        } else {
            Self::Lazy {
                size,
                modulus,
                passes: Vec::new(),
            }
        })
    }

    pub(crate) fn size(&self) -> u64 {
        match self {
            Self::Dense { values, .. } => values.len() as u64,
            Self::Lazy { size, .. } => *size,
        }
    }

    pub(crate) fn shuffle(&mut self, rng: &mut Rng) {
        match self {
            Self::Dense { values, modulus } => {
                for index in 0..values.len() {
                    let other = (rng.next() % *modulus) as usize;
                    values.swap(index, other);
                }
            }
            Self::Lazy { size, passes, .. } => {
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
            Self::Lazy {
                size,
                modulus,
                passes,
            } => {
                if index >= *size {
                    return None;
                }
                let mut position = index;
                for pass in passes.iter().rev() {
                    position = trace_pass(pass, *size, *modulus, position);
                }
                Some(position)
            }
        }
    }
}

fn draw_after(pass: &Pass, steps: u64) -> u64 {
    mul_mod(pass.start, pow_mod(MULTIPLIER, steps))
}

fn trace_pass(pass: &Pass, size: u64, modulus: u64, mut position: u64) -> u64 {
    let mut limit = size;
    loop {
        let mut event: Option<u64> = None;
        if position < limit {
            event = Some(position);
        }
        if position < modulus {
            if pass.start == 0 {
                if position == 0 && limit > 0 && event.is_none_or(|found| limit - 1 > found) {
                    event = Some(limit - 1);
                }
            } else {
                let mut candidate = position;
                while candidate < MODULUS {
                    if candidate != 0
                        && let Some(exponent) = discrete_log(mul_mod(candidate, pass.inverse_start))
                        && exponent >= 1
                        && exponent <= limit
                    {
                        let step = exponent - 1;
                        if event.is_none_or(|found| step > found) {
                            event = Some(step);
                        }
                    }
                    candidate += modulus;
                }
            }
        }
        let Some(step) = event else {
            return position;
        };
        let partner = draw_after(pass, step + 1) % modulus;
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
            assert_eq!(discrete_log(pow_mod(MULTIPLIER, exponent)), Some(exponent));
        }
    }

    #[test]
    fn the_key_modulus_truncates_to_an_int_as_c_does() {
        assert_eq!(key_modulus(1), Some(1));
        assert_eq!(key_modulus(2_147_483_647), Some(2_147_483_647));
        assert_eq!(key_modulus(2_147_483_648), Some(2_147_483_648));
        assert_eq!(key_modulus(3_000_000_000), Some(1_294_967_296));
        assert_eq!(key_modulus((1 << 32) + 5), Some(5));
        assert_eq!(key_modulus(1 << 32), None);
        assert_eq!(key_modulus(u64::MAX), Some(1));
    }

    fn reference(size: u64, modulus: u64, seed: u64, passes: usize) -> (Vec<u32>, Rng) {
        let mut rng = Rng::new(seed);
        let mut values: Vec<u32> = (0..size as u32).collect();
        for _ in 0..passes {
            for index in 0..values.len() {
                let other = (rng.next() % modulus) as usize;
                values.swap(index, other);
            }
        }
        (values, rng)
    }

    #[test]
    fn lazy_permutations_equal_dense_shuffles() {
        for (seed, size, modulus, passes) in [
            (1_u64, 1_200_000_u64, 1_200_000_u64, 1),
            (42, 1_500_000, 1_500_000, 3),
            (1_339_962_585, 1_100_000, 1_100_000, 2),
            (0, 1_100_000, 1_100_000, 2),
            (7, 1_100_000, 1_000_003, 2),
            (0, 1_100_000, 4_099, 2),
        ] {
            let (dense, dense_rng) = reference(size, modulus, seed, passes);
            let mut lazy_rng = Rng::new(seed);
            let mut lazy = KeyPermutation::Lazy {
                size,
                modulus,
                passes: Vec::new(),
            };
            for _ in 0..passes {
                lazy.shuffle(&mut lazy_rng);
            }
            assert_eq!(dense_rng.state(), lazy_rng.state());
            for index in (0..8).chain([size / 2, size - 1]) {
                assert_eq!(
                    lazy.entry(index),
                    Some(u64::from(dense[index as usize])),
                    "seed {seed} modulus {modulus} index {index}"
                );
            }
        }
    }
}
