use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{Rng, SeedableRng};

const DOMAIN: &[u8] = b"benchbox-datagen/synthetic/v1";
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const GOLDEN_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;
const SMALL_BOUND: u64 = 1 << 16;

fn fnv1a(parts: &[&[u8]]) -> u64 {
    let mut hash = FNV_OFFSET;
    for part in parts {
        for byte in *part {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME);
        }
        hash = (hash ^ 0xff).wrapping_mul(FNV_PRIME);
    }
    hash
}

fn mix(value: u64) -> u64 {
    let mut z = value.wrapping_add(GOLDEN_GAMMA);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Stream {
    base: u64,
    seed: u64,
}

impl Stream {
    pub(crate) fn new(benchmark: &str, table: &str, name: &str, seed: u64) -> Self {
        Self {
            base: mix(fnv1a(&[
                DOMAIN,
                benchmark.as_bytes(),
                table.as_bytes(),
                name.as_bytes(),
            ])),
            seed,
        }
    }

    pub(crate) fn block(&self, index: u64) -> Draw {
        let mut state = mix(mix(self.base ^ self.seed) ^ index);
        let mut key = [0_u8; 32];
        for index in 0..4 {
            state = state.wrapping_add(GOLDEN_GAMMA);
            key[index * 8..index * 8 + 8].copy_from_slice(&mix(state).to_le_bytes());
        }
        Draw(ChaCha8Rng::from_seed(key))
    }
}

pub(crate) struct Draw(ChaCha8Rng);

impl Draw {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn u32(&mut self) -> u32 {
        self.0.next_u32()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn u64(&mut self) -> u64 {
        self.0.next_u64()
    }

    pub(crate) fn below(&mut self, bound: u64) -> u64 {
        debug_assert!(bound > 0);
        if bound <= SMALL_BOUND {
            (u64::from(self.0.next_u32()) * bound) >> 32
        } else {
            ((u128::from(self.0.next_u64()) * u128::from(bound)) >> 64) as u64
        }
    }

    pub(crate) fn range(&mut self, low: i64, high: i64) -> i64 {
        debug_assert!(low <= high);
        low + self.below((high - low) as u64 + 1) as i64
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn unit_f64(&mut self) -> f64 {
        (self.0.next_u64() >> 11) as f64 * (1.0 / (1_u64 << 53) as f64)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn uniform_f64(&mut self, low: f64, high: f64) -> f64 {
        low + self.unit_f64() * (high - low)
    }

    pub(crate) fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn words(&mut self, list: &[&str], count: usize, separator: &str, out: &mut String) {
        for index in 0..count {
            if index > 0 {
                out.push_str(separator);
            }
            out.push_str(self.pick::<&str>(list));
        }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct Weighted {
    cumulative: Vec<u64>,
}

#[cfg_attr(not(test), allow(dead_code))]
impl Weighted {
    pub(crate) fn new(weights: &[u32]) -> Self {
        let mut total = 0_u64;
        let cumulative = weights
            .iter()
            .map(|weight| {
                total += u64::from(*weight);
                total
            })
            .collect::<Vec<_>>();
        assert!(total > 0, "weights must not sum to zero");
        Self { cumulative }
    }

    pub(crate) fn sample(&self, draw: &mut Draw) -> usize {
        let total = *self.cumulative.last().expect("weights are not empty");
        let target = draw.below(total);
        self.cumulative.partition_point(|upper| *upper <= target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streams_are_pinned_to_their_key_and_independent_of_each_other() {
        let first = Stream::new("ssb", "lineorder", "fields", 42).block(0).u64();
        assert_eq!(
            first,
            Stream::new("ssb", "lineorder", "fields", 42).block(0).u64()
        );
        let others = [
            Stream::new("ssb", "lineorder", "fields", 42).block(1).u64(),
            Stream::new("ssb", "lineorder", "fields", 43).block(0).u64(),
            Stream::new("ssb", "lineorder", "orders", 42).block(0).u64(),
            Stream::new("ssb", "part", "fields", 42).block(0).u64(),
            Stream::new("tpch", "lineorder", "fields", 42)
                .block(0)
                .u64(),
        ];
        for other in others {
            assert_ne!(first, other);
        }
    }

    #[test]
    fn the_first_draws_of_a_stream_are_frozen() {
        let mut draw = Stream::new("ssb", "customer", "fields", 42).block(7);
        let drawn = [
            draw.u64(),
            draw.u64(),
            draw.below(1_000_000),
            draw.range(-5, 5) as u64,
            u64::from(draw.u32()),
        ];
        assert_eq!(drawn, FROZEN);
    }

    const FROZEN: [u64; 5] = [
        2_380_616_159_500_965_076,
        1_057_932_929_285_887_809,
        820_476,
        0,
        1_296_364_849,
    ];

    #[test]
    fn bounded_draws_stay_in_range_and_cover_it() {
        let mut draw = Stream::new("t", "t", "t", 1).block(0);
        let mut seen = [0_u32; 7];
        for _ in 0..7_000 {
            seen[draw.range(3, 9) as usize - 3] += 1;
            assert!((0..1_000_000).contains(&draw.below(1_000_000)));
            assert!((0.0..1.0).contains(&draw.unit_f64()));
        }
        assert!(seen.iter().all(|count| *count > 800));
        let value = draw.uniform_f64(2.0, 4.0);
        assert!((2.0..4.0).contains(&value));
        let mut text = String::new();
        draw.words(&["a", "b"], 3, "-", &mut text);
        assert_eq!(text.len(), 5);
    }

    #[test]
    fn weighted_choices_follow_their_weights() {
        let weighted = Weighted::new(&[1, 0, 3]);
        let mut draw = Stream::new("t", "t", "t", 1).block(0);
        let mut counts = [0_u32; 3];
        for _ in 0..40_000 {
            counts[weighted.sample(&mut draw)] += 1;
        }
        assert_eq!(counts[1], 0);
        assert!((9_000..11_000).contains(&counts[0]));
    }
}
