use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use tpcdsgen::types::Decimal;

use super::calendar::date_string;
use crate::{Error, FileStats};

const BKEY_LEN: usize = 16;

pub(super) fn business_key(key: i64) -> String {
    let mut out = String::with_capacity(BKEY_LEN);
    for word in [(key as u64) >> 32, (key as u64) & 0xFFFF_FFFF] {
        let mut value = word;
        for _ in 0..8 {
            out.push((b'A' + (value & 0xF) as u8) as char);
            value >>= 4;
        }
    }
    out
}

pub(super) struct Out {
    table: &'static str,
    path: PathBuf,
    writer: BufWriter<File>,
    line: String,
    fields: usize,
    rows: u64,
    bytes: u64,
}

impl Out {
    pub(super) fn create(dir: &Path, name: &'static str, update: i32) -> Result<Self, Error> {
        let path = dir.join(format!("{name}_{update}.dat"));
        let file = File::create(&path).map_err(|e| io_error(&path, e))?;
        Ok(Self {
            table: name,
            path,
            writer: BufWriter::with_capacity(1 << 20, file),
            line: String::with_capacity(512),
            fields: 0,
            rows: 0,
            bytes: 0,
        })
    }

    fn separator(&mut self) {
        if self.fields > 0 {
            self.line.push('|');
        }
        self.fields += 1;
    }

    pub(super) fn text(&mut self, value: &str) -> &mut Self {
        self.separator();
        self.line.push_str(value);
        self
    }

    pub(super) fn int(&mut self, value: i32) -> &mut Self {
        self.separator();
        self.line.push_str(&value.to_string());
        self
    }

    pub(super) fn key(&mut self, value: i64) -> &mut Self {
        self.separator();
        if value != -1 {
            self.line.push_str(&value.to_string());
        }
        self
    }

    pub(super) fn id(&mut self, value: i64) -> &mut Self {
        self.separator();
        if value != -1 {
            self.line.push_str(&business_key(value));
        }
        self
    }

    pub(super) fn date(&mut self, julian: i32) -> &mut Self {
        self.separator();
        if julian > 0 {
            self.line.push_str(&date_string(julian));
        }
        self
    }

    pub(super) fn time(&mut self, seconds: i64) -> &mut Self {
        self.separator();
        let hours = seconds / 3600;
        let minutes = (seconds % 3600) / 60;
        let secs = seconds % 60;
        self.line
            .push_str(&format!("{hours:02}:{minutes:02}:{secs:02}"));
        self
    }

    pub(super) fn decimal(&mut self, value: &Decimal) -> &mut Self {
        self.separator();
        self.line.push_str(&value.to_string());
        self
    }

    pub(super) fn flag(&mut self, value: bool) -> &mut Self {
        self.separator();
        self.line.push(if value { 'Y' } else { 'N' });
        self
    }

    pub(super) fn end(&mut self) -> Result<(), Error> {
        self.line.push('\n');
        self.writer
            .write_all(self.line.as_bytes())
            .map_err(|e| io_error(&self.path, e))?;
        self.bytes += self.line.len() as u64;
        self.rows += 1;
        self.line.clear();
        self.fields = 0;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<FileStats, Error> {
        self.writer.flush().map_err(|e| io_error(&self.path, e))?;
        Ok(FileStats {
            table: self.table,
            path: self.path,
            rows: self.rows,
            bytes: self.bytes,
        })
    }
}

fn io_error(path: &Path, error: impl std::fmt::Display) -> Error {
    Error::Write(format!("{}: {error}", path.display()))
}
