use std::path::Path;

use super::columns::*;
use super::ctx::Ctx;
use super::out::Out;
use super::plan::Channel;
use crate::{Error, FileStats};

pub(super) fn write_dimensions(_ctx: &mut Ctx, _dir: &Path) -> Result<Vec<FileStats>, Error> {
    Ok(Vec::new())
}

fn first_sk(id: i64) -> i64 {
    match id % 3 {
        1 => (id / 3) * 6 + 1,
        2 => (id / 3) * 6 + 2,
        _ => (id / 3 - 1) * 6 + 4,
    }
}

pub(super) fn write_inventory(ctx: &mut Ctx, dir: &Path) -> Result<Vec<FileStats>, Error> {
    let mut out = Out::create(dir, "s_inventory", ctx.plan.update)?;
    let items = ctx.ids("item");
    let warehouses = ctx.rows("warehouse");
    for index in 1..=ctx.plan.rows(Channel::Inventory) {
        let mut n = index - 1;
        let item = n % items + 1;
        n /= items;
        let warehouse = n % warehouses + 1;
        let date = ctx.plan.update_date(Channel::Inventory, index);
        let quantity = ctx.rng.int(S_INVN_QUANTITY, 0, 250);
        out.id(warehouse)
            .id(first_sk(item))
            .date(date)
            .int(quantity)
            .end()?;
        ctx.rng.row_stop(TABLE_S_INVENTORY);
    }
    Ok(vec![out.finish()?])
}

const GMT_OFFSETS: [(i64, i32); 63] = [
    (599, -99),
    (999, -5),
    (2799, -5),
    (2999, -5),
    (3899, -5),
    (4999, -5),
    (5999, -5),
    (6999, -5),
    (8999, -5),
    (9999, -5),
    (14999, -5),
    (19699, -5),
    (19999, -5),
    (20599, -5),
    (21999, -5),
    (24699, -5),
    (26999, -5),
    (28999, -5),
    (29999, -5),
    (31999, -5),
    (33999, -5),
    (34999, -6),
    (34095, -5),
    (36999, -6),
    (38599, -5),
    (39999, -6),
    (41799, -5),
    (42799, -6),
    (45999, -5),
    (47999, -5),
    (49999, -5),
    (52999, -6),
    (54999, -6),
    (56999, -6),
    (57499, -6),
    (57999, -7),
    (58499, -6),
    (58499, -7),
    (59999, -7),
    (62999, -6),
    (65999, -6),
    (67999, -6),
    (68999, -6),
    (69999, -7),
    (71599, -6),
    (72999, -6),
    (74999, -6),
    (78999, -6),
    (79999, -7),
    (81999, -7),
    (83199, -7),
    (83699, -7),
    (83999, -8),
    (84999, -7),
    (86999, -7),
    (88999, -7),
    (89999, -8),
    (95999, -8),
    (96699, -8),
    (96899, -10),
    (97999, -8),
    (99499, -8),
    (99999, -9),
];

pub(super) fn write_zip_to_gmt(ctx: &mut Ctx, dir: &Path) -> Result<Vec<FileStats>, Error> {
    let mut out = Out::create(dir, "s_zip_to_gmt", ctx.plan.update)?;
    for index in 1..=super::dims::zip_rows(ctx.scale) {
        let zip = index - 1;
        let offset = GMT_OFFSETS
            .iter()
            .find(|(max, _)| zip <= *max)
            .map_or(-99, |(_, offset)| *offset);
        if offset == -99 {
            continue;
        }
        out.text(&format!("{zip:05}")).int(offset).end()?;
    }
    Ok(vec![out.finish()?])
}

pub(super) fn zip_rows(scale: f64) -> i64 {
    if scale < 1.0 {
        return ((100_000.0 * scale) as i64).max(1);
    }
    100_000
}
