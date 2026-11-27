use std::cell::OnceCell;

use super::DsqgenError;

const NAME_LEN: usize = 20;
const ENTRY_LEN: usize = NAME_LEN + 7 * 4;
pub(crate) const TYPE_INT: i32 = 7;

#[derive(Debug, Clone)]
struct Entry {
    name: String,
    offset: usize,
    str_space: usize,
    length: usize,
    weight_width: usize,
    value_width: usize,
    name_space: usize,
}

#[derive(Debug)]
pub(crate) struct Dist {
    pub(crate) name: String,
    pub(crate) length: usize,
    pub(crate) types: Vec<i32>,
    weights: Vec<Vec<i64>>,
    maxima: Vec<i64>,
    values: Vec<Vec<String>>,
    names: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct DistIndex<'a> {
    bytes: &'a [u8],
    entries: Vec<Entry>,
    loaded: Vec<OnceCell<Dist>>,
}

fn malformed(message: impl Into<String>) -> DsqgenError {
    DsqgenError::Distributions(message.into())
}

fn read_i32(bytes: &[u8], offset: usize) -> Result<i32, DsqgenError> {
    bytes
        .get(offset..offset + 4)
        .map(|slice| i32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
        .ok_or_else(|| malformed("the index is truncated"))
}

fn read_count(bytes: &[u8], offset: usize) -> Result<usize, DsqgenError> {
    usize::try_from(read_i32(bytes, offset)?).map_err(|_| malformed("a negative size was found"))
}

fn cstring(bytes: &[u8], offset: usize) -> Result<&str, DsqgenError> {
    let tail = bytes
        .get(offset..)
        .ok_or_else(|| malformed("a string offset is out of range"))?;
    let end = tail
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| malformed("a string is not terminated"))?;
    std::str::from_utf8(&tail[..end]).map_err(|_| malformed("a string is not UTF-8"))
}

impl<'a> DistIndex<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Result<Self, DsqgenError> {
        let count = read_count(bytes, 0)?;
        let index_len = count
            .checked_mul(ENTRY_LEN)
            .filter(|len| *len <= bytes.len().saturating_sub(4))
            .ok_or_else(|| malformed("the index entry count exceeds the file size"))?;
        let base = bytes.len() - index_len;
        let mut entries = Vec::with_capacity(count);
        for slot in 0..count {
            let start = base + slot * ENTRY_LEN;
            let raw = &bytes[start..start + NAME_LEN];
            let end = raw.iter().position(|byte| *byte == 0).unwrap_or(NAME_LEN);
            let name = std::str::from_utf8(&raw[..end])
                .map_err(|_| malformed("a distribution name is not UTF-8"))?
                .to_owned();
            let field = |position: usize| read_count(bytes, start + NAME_LEN + position * 4);
            entries.push(Entry {
                name,
                offset: field(1)?,
                str_space: field(2)?,
                length: field(3)?,
                weight_width: field(4)?,
                value_width: field(5)?,
                name_space: field(6)?,
            });
        }
        let loaded = entries.iter().map(|_| OnceCell::new()).collect();
        Ok(Self {
            bytes,
            entries,
            loaded,
        })
    }

    pub(crate) fn find(&self, name: &str) -> Result<Option<&Dist>, DsqgenError> {
        let Some(slot) = self
            .entries
            .iter()
            .position(|entry| entry.name.eq_ignore_ascii_case(name))
        else {
            return Ok(None);
        };
        if self.loaded[slot].get().is_none() {
            let dist = self.decode(&self.entries[slot])?;
            let _ = self.loaded[slot].set(dist);
        }
        Ok(self.loaded[slot].get())
    }

    fn decode(&self, entry: &Entry) -> Result<Dist, DsqgenError> {
        let bytes = self.bytes;
        let mut cursor = entry.offset;
        let mut take = |count: usize| -> Result<usize, DsqgenError> {
            let start = cursor;
            cursor = count
                .checked_mul(4)
                .and_then(|len| start.checked_add(len))
                .filter(|end| *end <= bytes.len())
                .ok_or_else(|| malformed(format!("distribution {} is truncated", entry.name)))?;
            Ok(start)
        };
        let types_at = take(entry.value_width)?;
        let types = (0..entry.value_width)
            .map(|slot| read_i32(bytes, types_at + slot * 4))
            .collect::<Result<Vec<_>, _>>()?;
        let mut weights = Vec::with_capacity(entry.weight_width);
        let mut maxima = Vec::with_capacity(entry.weight_width);
        for _ in 0..entry.weight_width {
            let at = take(entry.length)?;
            let mut running = 0_i64;
            let mut cumulative = Vec::with_capacity(entry.length);
            for row in 0..entry.length {
                running += i64::from(read_i32(bytes, at + row * 4)?);
                cumulative.push(running);
            }
            maxima.push(running);
            weights.push(cumulative);
        }
        let mut offsets = Vec::with_capacity(entry.value_width);
        for _ in 0..entry.value_width {
            let at = take(entry.length)?;
            offsets.push(
                (0..entry.length)
                    .map(|row| read_count(bytes, at + row * 4))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        let names_at = cursor;
        let strings_at = names_at
            .checked_add(entry.name_space)
            .ok_or_else(|| malformed("a name table is out of range"))?;
        let strings_end = strings_at
            .checked_add(entry.str_space)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| malformed(format!("distribution {} is truncated", entry.name)))?;
        let strings = &bytes[strings_at..strings_end];
        let values = offsets
            .iter()
            .map(|column| {
                column
                    .iter()
                    .map(|offset| cstring(strings, *offset).map(str::to_owned))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut names = Vec::new();
        if entry.name_space > 0 {
            let table = &bytes[names_at..strings_at];
            let mut start = 0;
            while start < table.len() {
                let end = table[start..]
                    .iter()
                    .position(|byte| *byte == 0)
                    .map_or(table.len(), |len| start + len);
                names.push(
                    std::str::from_utf8(&table[start..end])
                        .map_err(|_| malformed("a column name is not UTF-8"))?
                        .to_owned(),
                );
                start = end + 1;
            }
        }
        Ok(Dist {
            name: entry.name.clone(),
            length: entry.length,
            types,
            weights,
            maxima,
            values,
            names,
        })
    }
}

impl Dist {
    pub(crate) fn maximum(&self, weight_set: usize) -> Result<i64, DsqgenError> {
        weight_set
            .checked_sub(1)
            .and_then(|slot| self.maxima.get(slot))
            .copied()
            .ok_or_else(|| self.overrun())
    }

    pub(crate) fn value_type(&self, value_set: i64) -> Option<i32> {
        usize::try_from(value_set)
            .ok()
            .and_then(|set| set.checked_sub(1))
            .and_then(|slot| self.types.get(slot))
            .copied()
    }

    pub(crate) fn weight(&self, row: i64, weight_set: i64) -> Result<i64, DsqgenError> {
        let sets = usize::try_from(weight_set)
            .ok()
            .and_then(|set| set.checked_sub(1))
            .and_then(|slot| self.weights.get(slot));
        let index = usize::try_from(row).ok().and_then(|row| row.checked_sub(1));
        match (sets, index) {
            (Some(set), Some(index)) if index < set.len() => {
                Ok(set[index] - if index > 0 { set[index - 1] } else { 0 })
            }
            _ => Err(self.overrun()),
        }
    }

    pub(crate) fn member(&self, row: i64, value_set: i64) -> Result<&str, DsqgenError> {
        let column = usize::try_from(value_set)
            .ok()
            .and_then(|set| set.checked_sub(1))
            .and_then(|slot| self.values.get(slot));
        let index = usize::try_from(row).ok().and_then(|row| row.checked_sub(1));
        match (column, index) {
            (Some(column), Some(index)) if index < column.len() => Ok(&column[index]),
            _ => Err(self.overrun()),
        }
    }

    pub(crate) fn pick_row(&self, weight_set: i64, level: i64) -> Result<i64, DsqgenError> {
        let set = usize::try_from(weight_set)
            .ok()
            .and_then(|set| set.checked_sub(1))
            .and_then(|slot| self.weights.get(slot))
            .ok_or_else(|| self.overrun())?;
        set.iter()
            .position(|cumulative| level <= *cumulative)
            .map(|index| index as i64 + 1)
            .ok_or_else(|| self.overrun())
    }

    pub(crate) fn name_index(&self, name: &str, value_name: bool) -> i64 {
        if self.names.is_empty() {
            return -1;
        }
        let total = self.types.len() + self.weights.len();
        let mut found = 0;
        loop {
            if self
                .names
                .get(found)
                .is_some_and(|known| known.eq_ignore_ascii_case(name))
            {
                break;
            }
            found += 1;
            if found >= total {
                break;
            }
        }
        if value_name && found < self.types.len() {
            return found as i64 + 1;
        }
        if !value_name && found > self.types.len() {
            return (found - self.types.len()) as i64 + 1;
        }
        -1
    }

    fn overrun(&self) -> DsqgenError {
        DsqgenError::Evaluation(format!(
            "distribution {} was indexed out of range",
            self.name
        ))
    }
}
