mod calendar;
mod columns;
mod ctx;
mod dims;
mod orders;
mod out;
mod plan;
mod rng;

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::{Error, FileStats};
use calendar::date_string;
use ctx::Ctx;

pub(crate) use rng::DEFAULT_SEED;

fn write_delete_file(path: &Path, dates: &[i32; 6]) -> Result<FileStats, Error> {
    let mut text = String::new();
    for pair in dates.chunks(2) {
        text.push_str(&format!("{}|{}\n", date_string(pair[0]), date_string(pair[1])));
    }
    let io = |e: std::io::Error| Error::Write(format!("{}: {e}", path.display()));
    let mut file = File::create(path).map_err(io)?;
    file.write_all(text.as_bytes()).map_err(io)?;
    Ok(FileStats {
        table: "delete",
        path: path.to_path_buf(),
        rows: 3,
        bytes: text.len() as u64,
    })
}

pub(crate) fn write_tpcds_refresh(
    scale_factor: f64,
    sets: i32,
    seed: i32,
    output_dir: &Path,
) -> Result<Vec<FileStats>, Error> {
    fs::create_dir_all(output_dir)
        .map_err(|e| Error::Write(format!("{}: {e}", output_dir.display())))?;
    let mut stats = Vec::new();
    for update in 1..=sets {
        stats.extend(write_set(scale_factor, update, seed, output_dir)?);
    }
    Ok(stats)
}

fn write_set(
    scale_factor: f64,
    update: i32,
    seed: i32,
    dir: &Path,
) -> Result<Vec<FileStats>, Error> {
    let mut ctx = Ctx::new(scale_factor, update, seed);
    let mut stats = vec![
        write_delete_file(&dir.join(format!("delete_{update}.dat")), &ctx.plan.update_dates)?,
        write_delete_file(
            &dir.join(format!("inventory_delete_{update}.dat")),
            &ctx.plan.inventory_dates,
        )?,
    ];
    stats.extend(dims::write_dimensions(&mut ctx, dir)?);
    stats.extend(orders::write_catalog_orders(&mut ctx, dir)?);
    stats.extend(dims::write_inventory(&mut ctx, dir)?);
    stats.extend(orders::write_purchases(&mut ctx, dir)?);
    stats.extend(orders::write_web_orders(&mut ctx, dir)?);
    stats.extend(dims::write_zip_to_gmt(&mut ctx, dir)?);
    Ok(stats)
}
