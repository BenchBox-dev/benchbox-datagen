use tpcdsgen::random::{RandomNumberStream, RandomValueGenerator};

use super::columns::{Col, MAX_COLUMN, PER_ROW, Span};

pub(crate) const DEFAULT_SEED: i32 = 19_620_718;

pub(super) struct Rng {
    streams: Vec<RandomNumberStream>,
}

impl Rng {
    pub(super) fn new(seed: i32) -> Self {
        let streams = (0..=MAX_COLUMN)
            .map(|number| {
                let per_row = if number == 0 { 0 } else { PER_ROW[number] };
                RandomNumberStream::new_with_base(number as i32, seed, per_row)
            })
            .collect();
        Self { streams }
    }

    pub(super) fn stream(&mut self, column: Col) -> &mut RandomNumberStream {
        &mut self.streams[column.number]
    }

    pub(super) fn next(&mut self, column: Col) -> i64 {
        self.streams[column.number].next_random()
    }

    pub(super) fn int(&mut self, column: Col, min: i32, max: i32) -> i32 {
        RandomValueGenerator::generate_uniform_random_int(min, max, self.stream(column))
    }

    pub(super) fn key(&mut self, column: Col, min: i64, max: i64) -> i64 {
        RandomValueGenerator::generate_uniform_random_key(min, max, self.stream(column))
    }

    pub(super) fn row_stop(&mut self, span: Span) {
        for number in span.first..=span.last {
            let stream = &mut self.streams[number];
            while stream.get_seeds_used() < stream.get_seeds_per_row() {
                stream.next_random();
            }
            stream.reset_seeds_used();
        }
    }
}
