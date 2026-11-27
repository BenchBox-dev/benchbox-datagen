mod calendar;
mod columns;
mod ctx;
mod dims;
mod orders;
mod out;
mod plan;
mod rng;
mod scd;

use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use crate::{Error, FileStats};
use calendar::date_string;
use ctx::{Ctx, Run};

pub(crate) use rng::DEFAULT_SEED;

pub(crate) const REFRESH_SCALE_GRID: [u32; 9] =
    [1, 10, 100, 300, 1000, 3000, 10_000, 30_000, 100_000];

pub(crate) fn validate_refresh_scale(scale_factor: f64) -> Result<(), Error> {
    let on_grid = scale_factor < 1.0
        || REFRESH_SCALE_GRID
            .iter()
            .any(|&grid| f64::from(grid) == scale_factor);
    if on_grid {
        return Ok(());
    }
    let accepted = REFRESH_SCALE_GRID.map(|grid| grid.to_string()).join(", ");
    Err(Error::RefreshUnsupported(format!(
        "tpcds refresh sets at scale factor {scale_factor} are not supported: dsdgen -update \
         aborts for scale factors of 1 or more that are not on the scale grid; accepted scale \
         factors are below 1 or one of {accepted}"
    )))
}

fn write_delete_file(run: &Run, path: &Path, dates: &[i32; 6]) -> Result<FileStats, Error> {
    run.check()?;
    let mut text = String::new();
    for pair in dates.chunks(2) {
        text.push_str(&format!(
            "{}|{}\n",
            date_string(pair[0]),
            date_string(pair[1])
        ));
    }
    let io = |e: std::io::Error| Error::Write(format!("{}: {e}", path.display()));
    let mut file = run.create_file(path).map_err(io)?;
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
    cancel: &AtomicBool,
) -> Result<Vec<FileStats>, Error> {
    fs::create_dir_all(output_dir)
        .map_err(|e| Error::Write(format!("{}: {e}", output_dir.display())))?;
    let run = Run::new(cancel);
    let outcome = (1..=sets).try_fold(Vec::new(), |mut stats, update| {
        stats.extend(write_set(&run, scale_factor, update, seed, output_dir)?);
        Ok(stats)
    });
    if outcome.is_err() {
        run.remove_created();
    }
    outcome
}

fn write_set(
    run: &Run,
    scale_factor: f64,
    update: i32,
    seed: i32,
    dir: &Path,
) -> Result<Vec<FileStats>, Error> {
    let mut ctx = Ctx::new(run, scale_factor, update, seed)?;
    let mut stats = vec![
        write_delete_file(
            ctx.run,
            &dir.join(format!("delete_{update}.dat")),
            &ctx.plan.update_dates,
        )?,
        write_delete_file(
            ctx.run,
            &dir.join(format!("inventory_delete_{update}.dat")),
            &ctx.plan.inventory_dates,
        )?,
    ];
    stats.extend(dims::write_dimensions(&mut ctx, dir)?);
    stats.extend(scd::write_scd_tables(&mut ctx, dir)?);
    stats.extend(orders::write_catalog_orders(&mut ctx, dir)?);
    stats.extend(dims::write_inventory(&mut ctx, dir)?);
    stats.extend(orders::write_purchases(&mut ctx, dir)?);
    stats.extend(orders::write_web_orders(&mut ctx, dir)?);
    stats.extend(dims::write_zip_to_gmt(&mut ctx, dir)?);
    stats.retain(|file| file.rows > 0);
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    const SF1_SET1: [(&str, u64, u64); 23] = [
        ("delete_1.dat", 3, 66),
        ("inventory_delete_1.dat", 3, 66),
        ("s_call_center_1.dat", 1, 80),
        ("s_catalog_order_1.dat", 682, 115_689),
        ("s_catalog_order_lineitem_1.dat", 6138, 586_260),
        ("s_catalog_page_1.dat", 150, 21_151),
        ("s_catalog_returns_1.dat", 595, 114_947),
        ("s_customer_1.dat", 5000, 1_562_434),
        ("s_customer_address_1.dat", 25, 2351),
        ("s_inventory_1.dat", 270_000, 13_111_658),
        ("s_item_1.dat", 500, 87_583),
        ("s_promotion_1.dat", 5, 586),
        ("s_purchase_1.dat", 1022, 141_578),
        ("s_purchase_lineitem_1.dat", 12_264, 1_296_194),
        ("s_store_1.dat", 1, 97),
        ("s_store_returns_1.dat", 1200, 153_515),
        ("s_warehouse_1.dat", 1, 44),
        ("s_web_order_1.dat", 256, 43_308),
        ("s_web_order_lineitem_1.dat", 3072, 320_828),
        ("s_web_page_1.dat", 6, 474),
        ("s_web_returns_1.dat", 320, 45_480),
        ("s_web_site_1.dat", 1, 64),
        ("s_zip_to_gmt_1.dat", 99_400, 894_800),
    ];

    fn read(dir: &Path, name: &str) -> String {
        fs::read_to_string(dir.join(name)).expect("refresh file")
    }

    #[test]
    fn scale_factor_one_set_one_has_the_dsdgen_file_set_and_sizes() {
        let dir = tempfile::tempdir().unwrap();
        let stats =
            write_tpcds_refresh(1.0, 1, DEFAULT_SEED, dir.path(), &AtomicBool::new(false)).unwrap();
        let mut seen: Vec<(String, u64, u64)> = stats
            .iter()
            .map(|s| {
                (
                    s.path.file_name().unwrap().to_string_lossy().into_owned(),
                    s.rows,
                    s.bytes,
                )
            })
            .collect();
        seen.sort();
        let expected: Vec<(String, u64, u64)> = SF1_SET1
            .iter()
            .map(|(name, rows, bytes)| ((*name).to_owned(), *rows, *bytes))
            .collect();
        assert_eq!(seen, expected);
        for (name, _, bytes) in SF1_SET1 {
            assert_eq!(
                fs::metadata(dir.path().join(name)).unwrap().len(),
                bytes,
                "{name}"
            );
        }
    }

    #[test]
    fn scale_factor_one_files_start_with_the_dsdgen_rows() {
        let dir = tempfile::tempdir().unwrap();
        write_tpcds_refresh(1.0, 2, DEFAULT_SEED, dir.path(), &AtomicBool::new(false)).unwrap();
        assert_eq!(
            read(dir.path(), "delete_1.dat"),
            "2000-05-20|2000-05-21\n1999-09-18|1999-09-19\n2002-11-12|2002-11-13\n"
        );
        assert_eq!(
            read(dir.path(), "inventory_delete_2.dat"),
            "2001-03-22|2001-03-29\n2000-08-17|2000-08-24\n1999-11-25|1999-12-02\n"
        );
        assert!(read(dir.path(), "s_purchase_1.dat").starts_with(
            "1|AAAAAAAAEAAAAAAA|AAAAAAAAELMNAAAA|2000-05-20|0|10|244|So sound alternatives"
        ));
        assert!(
            read(dir.path(), "s_purchase_2.dat")
                .starts_with("1|AAAAAAAAEAAAAAAA|AAAAAAAAELMNAAAA|2001-03-23|0|10|244|")
        );
        assert_eq!(
            read(dir.path(), "s_inventory_2.dat").lines().next(),
            Some("AAAAAAAABAAAAAAA|AAAAAAAABAAAAAAA|2001-03-22|178")
        );
        assert_eq!(
            read(dir.path(), "s_zip_to_gmt_1.dat").lines().next(),
            Some("00600|-5")
        );
    }

    #[test]
    fn the_rng_seed_changes_the_set_and_is_deterministic() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        write_tpcds_refresh(0.01, 1, 20_260_709, first.path(), &AtomicBool::new(false)).unwrap();
        write_tpcds_refresh(0.01, 1, 20_260_709, second.path(), &AtomicBool::new(false)).unwrap();
        write_tpcds_refresh(0.01, 1, DEFAULT_SEED, other.path(), &AtomicBool::new(false)).unwrap();
        for name in ["s_purchase_1.dat", "s_customer_1.dat", "delete_1.dat"] {
            assert_eq!(
                read(first.path(), name),
                read(second.path(), name),
                "{name}"
            );
            assert_ne!(read(first.path(), name), read(other.path(), name), "{name}");
        }
    }

    fn entries(dir: &Path) -> Vec<String> {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn a_cancelled_run_leaves_no_files() {
        let dir = tempfile::tempdir().unwrap();
        let cancelled = AtomicBool::new(true);
        let result = write_tpcds_refresh(0.01, 2, DEFAULT_SEED, dir.path(), &cancelled);
        assert!(matches!(result, Err(Error::Cancelled)));
        assert!(entries(dir.path()).is_empty());
    }

    #[test]
    fn a_run_cancelled_midway_removes_the_files_it_created() {
        let dir = tempfile::tempdir().unwrap();
        let cancel = AtomicBool::new(false);
        let run = Run::new(&cancel);
        let mut out = out::Out::create(&run, dir.path(), "s_purchase", 1).unwrap();
        out.int(1).end().unwrap();
        cancel.store(true, Ordering::Relaxed);
        assert!(matches!(out.end(), Err(Error::Cancelled)));
        run.remove_created();
        assert!(entries(dir.path()).is_empty());
    }

    #[test]
    fn scale_factors_off_the_dsdgen_grid_are_rejected_before_any_file_is_written() {
        for scale in [2.0, 3.0, 11.0, 13.0, 301.0, 65_536.0, 100_001.0] {
            let root = tempfile::tempdir().unwrap();
            let dir = root.path().join("out");
            let result = crate::write_refresh_sets(
                crate::Benchmark::Tpcds,
                scale,
                1,
                None,
                &dir,
                &AtomicBool::new(false),
            );
            match result {
                Err(Error::RefreshUnsupported(message)) => {
                    assert!(message.contains("1, 10, 100, 300"), "{message}");
                }
                other => panic!("scale {scale}: {other:?}"),
            }
            assert!(!dir.exists(), "scale {scale} created the output directory");
        }
    }

    #[test]
    fn grid_scale_factors_and_scale_factors_below_one_are_accepted() {
        for scale in [0.0001, 0.5, 0.99, 1.0, 10.0, 100.0, 300.0, 100_000.0] {
            assert!(validate_refresh_scale(scale).is_ok(), "{scale}");
        }
    }

    #[test]
    fn a_failed_run_does_not_delete_a_file_it_did_not_create() {
        let dir = tempfile::tempdir().unwrap();
        let existing = dir.path().join("s_purchase_1.dat");
        fs::write(&existing, "keep\n").unwrap();
        let cancel = AtomicBool::new(false);
        let run = Run::new(&cancel);
        let mut owned = out::Out::create(&run, dir.path(), "s_customer", 1).unwrap();
        owned.int(1).end().unwrap();
        let mut shared = out::Out::create(&run, dir.path(), "s_purchase", 1).unwrap();
        shared.int(1).end().unwrap();
        run.remove_created();
        assert!(existing.exists());
        assert!(!dir.path().join("s_customer_1.dat").exists());
    }

    #[test]
    fn tables_without_rows_write_no_file_like_dsdgen() {
        let dir = tempfile::tempdir().unwrap();
        let stats =
            write_tpcds_refresh(0.0001, 1, DEFAULT_SEED, dir.path(), &AtomicBool::new(false))
                .unwrap();
        let mut written = entries(dir.path());
        written.sort();
        let mut reported: Vec<String> = stats
            .iter()
            .map(|s| s.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        reported.sort();
        assert_eq!(written, reported);
        assert_eq!(written.len(), 13);
        assert!(!written.contains(&"s_purchase_1.dat".to_owned()));
    }

    #[test]
    fn checked_helpers_return_errors_instead_of_panicking() {
        assert!(ctx::quotient(1, 0, "x").is_err());
        assert!(ctx::remainder(1, 0, "x").is_err());
        assert!(ctx::element(&[1, 2], -1, "x").is_err());
        assert!(ctx::element(&[1, 2], 2, "x").is_err());
        assert_eq!(ctx::element(&[1, 2], 1, "x").unwrap(), 2);
    }
}
