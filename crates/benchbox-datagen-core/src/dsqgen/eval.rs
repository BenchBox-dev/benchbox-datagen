use std::rc::Rc;

use super::DsqgenError;
use super::date::{Date, is_leap};
use super::idx::{DistIndex, TYPE_INT};
use super::lexer::Kw;
use super::parser::{
    Arena, Expr, ExprKind, Func, Op, SUFFIX_LIST, SUFFIX_RANGE, SUFFIX_ULIST, Template,
};
use super::rng::{KeyPermutation, Rng};

const DIST_UNIFORM: i64 = 1;
const DIST_SALES: i64 = 3;
const DIST_RETURNS: i64 = 5;
const MAX_ARGS: usize = 20;
const CALENDAR: &str = "calendar";

type Generated = (String, Vec<(String, String)>, bool);

#[derive(Debug, Clone, Default)]
pub(crate) struct Val {
    pub(crate) use_int: bool,
    pub(crate) n: i64,
    pub(crate) s: String,
    pub(crate) query_number: i64,
}

impl Val {
    pub(crate) fn render(&self) -> String {
        if self.use_int {
            self.n.to_string()
        } else {
            self.s.clone()
        }
    }
}

#[derive(Debug)]
enum Permutation {
    Empty,
    Rows(Vec<u32>),
    Keys(KeyPermutation),
}

pub(crate) struct Run<'a, 'b> {
    pub(crate) arena: Arena,
    pub(crate) templates: Vec<Template>,
    initialised: Vec<bool>,
    dists: &'a DistIndex<'b>,
    rng: Rng,
    permutation_rng: Rng,
    permutations: Vec<Permutation>,
    query_count: i64,
    pub(crate) query_number: i64,
    pub(crate) stream_number: i64,
    current: usize,
    template_name: String,
    wrapped: bool,
}

fn evaluation(message: impl Into<String>) -> DsqgenError {
    DsqgenError::Evaluation(message.into())
}

fn atoi(text: &str) -> i64 {
    let trimmed = text.trim_start();
    let end = trimmed
        .char_indices()
        .take_while(|(at, character)| {
            character.is_ascii_digit() || (*at == 0 && matches!(character, '-' | '+'))
        })
        .count();
    trimmed[..end].parse().unwrap_or(0)
}

fn key_uniform(rng: &mut Rng, min: i64, max: i64) -> Result<i64, DsqgenError> {
    let span = (max - min + 1) as i32;
    if span == 0 {
        return Err(evaluation("a random range has no values"));
    }
    let drawn = (rng.next() as i32) % span;
    Ok(i64::from(drawn.wrapping_add(min as i32)))
}

fn integer_uniform(rng: &mut Rng, min: i64, max: i64) -> Result<i64, DsqgenError> {
    if max - min + 1 == 0 {
        return Err(evaluation("a random range has no values"));
    }
    Ok(rng.integer(min, max))
}

impl<'a, 'b> Run<'a, 'b> {
    pub(crate) fn new(dists: &'a DistIndex<'b>, arena: Arena, seed: u64) -> Self {
        Self {
            arena,
            templates: Vec::new(),
            initialised: Vec::new(),
            dists,
            rng: Rng::new(seed),
            permutation_rng: Rng::new(0),
            permutations: Vec::new(),
            query_count: 1,
            query_number: 0,
            stream_number: 0,
            current: 0,
            template_name: String::new(),
            wrapped: false,
        }
    }

    pub(crate) fn add_template(&mut self, template: Template) {
        self.templates.push(template);
        self.initialised.push(false);
    }

    pub(crate) fn reset_permutation_rng(&mut self, seed: u64) {
        self.permutation_rng.reset(seed);
    }

    pub(crate) fn shuffle_order(&mut self, order: &mut [u32]) -> Result<(), DsqgenError> {
        let size = order.len() as i64;
        for index in 0..order.len() {
            let other = integer_uniform(&mut self.permutation_rng, 0, size - 1)? as usize;
            order.swap(index, other);
        }
        Ok(())
    }

    pub(crate) fn generate(
        &mut self,
        template: usize,
        log: &mut Option<String>,
    ) -> Result<Generated, DsqgenError> {
        self.current = template;
        self.wrapped = false;
        self.template_name = self.templates[template].name.clone();
        if let Some(log) = log {
            log.push_str(&format!("Template: {}\n", self.template_name));
        }
        if !self.initialised[template] {
            for id in self.templates[template].order.clone() {
                let sub = &mut self.arena.subs[id];
                let count = sub.n_use.max(1) * sub.n_parts.max(1);
                sub.vals = vec![Val::default(); count];
            }
            self.initialised[template] = true;
        }
        let mut parameters = Vec::new();
        let mut position = 0;
        while position < self.templates[template].order.len() {
            let id = self.templates[template].order[position];
            let sub = &mut self.arena.subs[id];
            let count = sub.n_use.max(1) * sub.n_parts.max(1);
            if sub.vals.len() != count {
                sub.vals = vec![Val::default(); count];
            }
            for val in &mut sub.vals {
                val.s.clear();
            }
            self.evaluate_sub(id, self.query_count)?;
            let sub = &self.arena.subs[id];
            for (index, val) in sub.vals.iter().enumerate() {
                let name = format!("{}.{:02}", sub.name, index + 1);
                let value = val.render();
                if let Some(log) = log {
                    log.push_str(&format!("\t{name} = {value}\n"));
                }
                if !name.starts_with('_') {
                    parameters.push((name, value));
                }
            }
            position = self.templates[template]
                .order
                .iter()
                .position(|candidate| *candidate == id)
                .map_or(position + 1, |at| at + 1);
        }
        let mut text = String::new();
        for segment in &self.templates[template].segments {
            text.push_str(&segment.text);
            if let Some(id) = segment.sub {
                let sub = &self.arena.subs[id];
                let index = sub.n_parts as i64 * segment.count as i64 + segment.use_index;
                let val = usize::try_from(index)
                    .ok()
                    .and_then(|index| sub.vals.get(index))
                    .ok_or_else(|| {
                        evaluation(format!(
                            "substitution {} has no value at position {index}",
                            sub.name
                        ))
                    })?;
                text.push_str(&val.render());
            }
            if segment.end_of_statement {
                text.push_str(";\n");
            }
        }
        self.query_count += 1;
        Ok((text, parameters, self.wrapped))
    }

    fn evaluate_sub(&mut self, id: usize, query: i64) -> Result<(), DsqgenError> {
        let expr = Rc::clone(&self.arena.subs[id].expr);
        let mut vals = std::mem::take(&mut self.arena.subs[id].vals);
        if vals.is_empty() {
            return Err(evaluation(format!(
                "substitution {} refers to itself",
                self.arena.subs[id].name
            )));
        }
        let outcome = self.eval_expr(&expr, &mut vals, false, query);
        self.arena.subs[id].vals = vals;
        outcome
    }

    fn eval_expr(
        &mut self,
        expr: &Expr,
        out: &mut [Val],
        is_param: bool,
        query: i64,
    ) -> Result<(), DsqgenError> {
        match &expr.kind {
            ExprKind::Str(text) => {
                out[0].s.push_str(text);
                out[0].use_int = false;
                out[0].query_number = query;
                return Ok(());
            }
            ExprKind::Int(value) => {
                out[0].n = *value;
                out[0].use_int = true;
                out[0].query_number = query;
                return Ok(());
            }
            ExprKind::Keyword(kw) => {
                self.eval_keyword(*kw, out)?;
                out[0].query_number = query;
                return Ok(());
            }
            _ => {}
        }
        if out[0].query_number == query {
            return Ok(());
        }
        match &expr.kind {
            ExprKind::Repl { text, weight } => {
                out[0].n = *weight;
                out[0].s.push_str(text);
            }
            ExprKind::Subst { name, element } => {
                let id = self.templates[self.current]
                    .find_sub(&mut self.arena, name)
                    .ok_or_else(|| evaluation(format!("substitution {name} is not defined")))?;
                if self.arena.subs[id].query_number != query {
                    self.evaluate_sub(id, query)?;
                }
                let existing = self.arena.subs[id]
                    .vals
                    .get(element - 1)
                    .ok_or_else(|| {
                        evaluation(format!("substitution {name} has no element {element}"))
                    })?
                    .clone();
                out[0] = existing;
                self.arena.subs[id].query_number = query;
            }
            ExprKind::Func { func, args } => {
                let mut params = Vec::with_capacity(args.len());
                for arg in args {
                    let mut value = Val::default();
                    self.eval_expr(arg, std::slice::from_mut(&mut value), true, query)?;
                    params.push(value);
                }
                if params.len() > MAX_ARGS {
                    return Err(evaluation("too many function arguments"));
                }
                match func {
                    Func::Text => self.eval_text(expr, args, out, is_param)?,
                    Func::Date => self.eval_date(expr, &params, out, is_param)?,
                    Func::Dist | Func::DistMember | Func::DistWeight => {
                        self.eval_dist(expr, *func, &params, out, is_param)?;
                    }
                    Func::Random => self.eval_random(expr, &params, out, is_param)?,
                }
            }
            ExprKind::Arith { op, args } => {
                let mut params = Vec::with_capacity(args.len());
                for arg in args {
                    let mut value = Val::default();
                    self.eval_expr(arg, std::slice::from_mut(&mut value), true, query)?;
                    params.push(value);
                }
                eval_arithmetic(*op, &params, &mut out[0])?;
            }
            ExprKind::Str(_) | ExprKind::Int(_) | ExprKind::Keyword(_) => {}
        }
        out[0].query_number = query;
        Ok(())
    }

    fn eval_keyword(&mut self, kw: Kw, out: &mut [Val]) -> Result<(), DsqgenError> {
        match kw {
            Kw::Query => {
                out[0].n = self.query_number;
                out[0].use_int = true;
            }
            Kw::Template => {
                out[0].s.push_str(&self.template_name);
                out[0].use_int = false;
            }
            Kw::Stream => {
                out[0].n = self.stream_number;
                out[0].use_int = true;
            }
            Kw::Seed => {
                out[0].n = self.rng.state() as i64;
                out[0].use_int = true;
            }
            Kw::Sales => {
                out[0].n = DIST_SALES;
                out[0].use_int = true;
            }
            Kw::Returns => {
                out[0].n = DIST_RETURNS;
                out[0].use_int = true;
            }
            Kw::Uniform => {
                out[0].n = DIST_UNIFORM;
                out[0].use_int = true;
            }
            Kw::Limit => {
                out[0].n = atoi(&self.arena.limit_text);
                out[0].use_int = true;
            }
            Kw::LimitA | Kw::LimitB | Kw::LimitC => {
                let letter = match kw {
                    Kw::LimitA => 'A',
                    Kw::LimitB => 'B',
                    _ => 'C',
                };
                let name = format!("__LIMIT{letter}");
                let id = self.templates[self.current]
                    .find_sub(&mut self.arena, &name)
                    .ok_or_else(|| evaluation(format!("substitution {name} is not defined")))?;
                let expr = Rc::clone(&self.arena.subs[id].expr);
                let query = self.query_number;
                self.eval_expr(&expr, &mut out[..1], false, query)?;
                if out[0].s.contains('%') {
                    self.templates[self.current].find_sub(&mut self.arena, "_LIMIT");
                    let limit = atoi(&self.arena.limit_text).to_string();
                    if out[0].s.matches('%').count() != 1 || !out[0].s.contains("%d") {
                        return Err(evaluation("a limit format must contain one %d"));
                    }
                    out[0].s = out[0].s.replacen("%d", &limit, 1);
                }
            }
            other => return Err(evaluation(format!("{other:?} is not a value keyword"))),
        }
        Ok(())
    }

    fn eval_text(
        &mut self,
        expr: &Expr,
        args: &[Expr],
        out: &mut [Val],
        is_param: bool,
    ) -> Result<(), DsqgenError> {
        if expr.suffix != 0 {
            let _ = is_param;
            return Err(evaluation("list modifiers are not supported on text()"));
        }
        let weight = |arg: &Expr| match &arg.kind {
            ExprKind::Repl { weight, .. } => *weight,
            _ => 0,
        };
        let total: i64 = args.iter().map(weight).sum();
        let mut level = integer_uniform(&mut self.rng, 1, total)?;
        for arg in args {
            if level <= weight(arg) {
                if let ExprKind::Repl { text, .. } = &arg.kind {
                    out[0].s.push_str(text);
                }
                return Ok(());
            }
            level -= weight(arg);
        }
        Err(evaluation("text() ran past its replacements"))
    }

    fn eval_date(
        &mut self,
        expr: &Expr,
        params: &[Val],
        out: &mut [Val],
        is_param: bool,
    ) -> Result<(), DsqgenError> {
        if expr.suffix != 0 {
            let _ = is_param;
            return Err(evaluation("list modifiers are not supported on date()"));
        }
        let begin =
            Date::parse(&params[0].s).ok_or_else(|| evaluation("an invalid date was supplied"))?;
        let end =
            Date::parse(&params[1].s).ok_or_else(|| evaluation("an invalid date was supplied"))?;
        let dist = params[2].n;
        let result = self.random_date(dist, begin, end)?;
        out[0].s.push_str(&result.format());
        out[0].use_int = false;
        Ok(())
    }

    fn random_date(&mut self, dist: i64, begin: Date, end: Date) -> Result<Date, DsqgenError> {
        let mut julian = begin.julian();
        let mut range = end.julian() - julian;
        match dist {
            DIST_SALES | DIST_RETURNS => {
                let calendar = self
                    .dists
                    .find(CALENDAR)?
                    .ok_or_else(|| evaluation("the calendar distribution is missing"))?;
                let mut day = begin.day;
                let mut year = begin.year;
                let mut total = 0;
                if range < 2 {
                    return Err(evaluation("the date range is too short"));
                }
                loop {
                    range -= 1;
                    if range == 0 {
                        break;
                    }
                    total += calendar.weight(day, dist + i64::from(is_leap(year)))?;
                    if day == 365 + i64::from(is_leap(year)) {
                        year += 1;
                        day = 1;
                    } else {
                        day += 1;
                    }
                }
                let mut remaining = integer_uniform(&mut self.rng, 1, total)?;
                day = begin.day;
                julian = begin.julian();
                year = begin.year;
                while remaining >= 0 {
                    remaining -= calendar.weight(day, dist + i64::from(is_leap(year)))?;
                    day += 1;
                    julian += 1;
                    if day > 365 + i64::from(is_leap(year)) {
                        year += 1;
                        day = 1;
                    }
                }
            }
            DIST_UNIFORM => {
                julian += integer_uniform(&mut self.rng, 0, range)?;
            }
            _ => return Err(evaluation("unsupported date distribution")),
        }
        Date::from_julian(julian).ok_or_else(|| evaluation("a generated date is out of range"))
    }

    fn eval_random(
        &mut self,
        expr: &Expr,
        params: &[Val],
        out: &mut [Val],
        is_param: bool,
    ) -> Result<(), DsqgenError> {
        let min = params[0].n;
        let max = params[1].n;
        match expr.suffix {
            0 => {
                out[0].n = key_uniform(&mut self.rng, min, max)?;
                out[0].use_int = true;
            }
            SUFFIX_LIST => {
                if is_param {
                    return Err(evaluation("a list cannot be a function argument"));
                }
                let span = (max - min + 1) as i32;
                for index in 0..params[3].n.max(0) as usize {
                    let drawn = key_uniform(&mut self.rng, min, max)?;
                    let slot = out
                        .get_mut(index)
                        .ok_or_else(|| evaluation("a list is larger than its substitution"))?;
                    slot.n = i64::from((drawn as i32).wrapping_rem(span).wrapping_add(min as i32));
                    slot.use_int = true;
                }
            }
            SUFFIX_ULIST => {
                if is_param {
                    return Err(evaluation("a list cannot be a function argument"));
                }
                let size = max - min + 1;
                if size <= 0 {
                    return Err(evaluation("a unique list has no values to permute"));
                }
                let slot = expr.perm_slot;
                if self.permutations.len() <= slot {
                    self.permutations
                        .resize_with(slot + 1, || Permutation::Empty);
                }
                if matches!(self.permutations[slot], Permutation::Empty) {
                    self.permutations[slot] = Permutation::Keys(KeyPermutation::new(size as u64));
                }
                let Permutation::Keys(keys) = &mut self.permutations[slot] else {
                    return Err(evaluation("a permutation slot was reused"));
                };
                keys.shuffle(&mut self.rng);
                let count = params[3].n.max(0) as u64;
                let held = keys.size();
                if count > held {
                    self.wrapped = true;
                }
                for index in 0..count {
                    let entry = keys.entry(index % held).unwrap_or(0) + 1;
                    let target = out
                        .get_mut(index as usize)
                        .ok_or_else(|| evaluation("a list is larger than its substitution"))?;
                    target.n = (entry as i64 % size) + min;
                    target.use_int = true;
                }
            }
            SUFFIX_RANGE => return Err(evaluation("range() is not supported")),
            _ => return Err(evaluation("list modifiers cannot be combined")),
        }
        Ok(())
    }

    fn eval_dist(
        &mut self,
        expr: &Expr,
        func: Func,
        params: &[Val],
        out: &mut [Val],
        is_param: bool,
    ) -> Result<(), DsqgenError> {
        let dists = self.dists;
        let name = params[0].s.as_str();
        let dist = dists.find(name)?;
        let named = |param: &Val, value: bool| -> i64 {
            if param.use_int {
                param.n
            } else {
                dist.map_or(-1, |dist| dist.name_index(&param.s, value))
            }
        };
        let row = named(&params[1], true);
        let column = named(&params[2], false);
        let dist =
            dist.ok_or_else(|| evaluation(format!("distribution {name:?} does not exist")))?;
        match func {
            Func::DistMember => {
                if dist.value_type(column) == Some(TYPE_INT) {
                    out[0].n = atoi(dist.member(row, column)?);
                    out[0].use_int = true;
                } else {
                    out[0].s.push_str(dist.member(row, column)?);
                }
            }
            Func::DistWeight => {
                out[0].n = dist.weight(row, column)?;
                out[0].use_int = true;
            }
            _ => {
                let integer = dist.value_type(row) == Some(TYPE_INT);
                match expr.suffix {
                    0 => {
                        let level =
                            integer_uniform(&mut self.rng, 1, dist.maximum(column as usize)?)?;
                        let picked = dist.pick_row(column, level)?;
                        let text = dist.member(picked, row)?;
                        if integer {
                            out[0].n = atoi(text);
                            out[0].use_int = true;
                        } else {
                            out[0].s.push_str(text);
                        }
                    }
                    SUFFIX_LIST => {
                        if is_param {
                            return Err(evaluation("a list cannot be a function argument"));
                        }
                        for index in 0..params[3].n.max(0) as usize {
                            let picked = integer_uniform(&mut self.rng, 1, dist.length as i64)?;
                            let text = dist.member(picked, row)?;
                            let slot = out.get_mut(index).ok_or_else(|| {
                                evaluation("a list is larger than its substitution")
                            })?;
                            if integer {
                                slot.n = atoi(text);
                                slot.use_int = true;
                            } else {
                                slot.s.push_str(text);
                            }
                        }
                    }
                    SUFFIX_ULIST => {
                        if is_param {
                            return Err(evaluation("a list cannot be a function argument"));
                        }
                        self.unique_dist_list(expr, dist, row, integer, params[3].n, out)?;
                    }
                    SUFFIX_RANGE => return Err(evaluation("range() is not supported")),
                    _ => return Err(evaluation("list modifiers cannot be combined")),
                }
            }
        }
        Ok(())
    }

    fn unique_dist_list(
        &mut self,
        expr: &Expr,
        dist: &super::idx::Dist,
        row: i64,
        integer: bool,
        wanted: i64,
        out: &mut [Val],
    ) -> Result<(), DsqgenError> {
        let size = dist.length;
        if wanted > size as i64 || size == 0 {
            return Err(evaluation("a unique list is larger than its distribution"));
        }
        let slot = expr.perm_slot;
        if self.permutations.len() <= slot {
            self.permutations
                .resize_with(slot + 1, || Permutation::Empty);
        }
        if matches!(self.permutations[slot], Permutation::Empty) {
            self.permutations[slot] = Permutation::Rows((0..size as u32).collect());
        }
        let Permutation::Rows(rows) = &mut self.permutations[slot] else {
            return Err(evaluation("a permutation slot was reused"));
        };
        for index in 0..rows.len() {
            let other = integer_uniform(&mut self.rng, 0, size as i64 - 1)? as usize;
            rows.swap(index, other);
        }
        let entry = |position: usize| -> Result<i64, DsqgenError> {
            rows.get(position - 1)
                .map(|value| i64::from(*value) + 1)
                .ok_or_else(|| evaluation("a unique list ran past its permutation"))
        };
        let mut count = 0_usize;
        let mut cursor = 1_usize;
        while (count as i64) < wanted {
            let target = out
                .get_mut(count)
                .ok_or_else(|| evaluation("a list is larger than its substitution"))?;
            if integer {
                target.use_int = true;
                target.n = atoi(dist.member(entry(cursor)?, row)?);
                cursor += 1;
            } else {
                let mut chosen = dist.member(entry(cursor)?, row)?.to_owned();
                cursor += 1;
                let mut at = 0;
                while count > 0 && at < count {
                    at = 0;
                    while at < count {
                        if out[at].s == chosen {
                            chosen = dist.member(entry(cursor)?, row)?.to_owned();
                            cursor += 1;
                            break;
                        }
                        at += 1;
                    }
                }
                out[count].s.push_str(&chosen);
            }
            count += 1;
        }
        Ok(())
    }
}

fn eval_arithmetic(op: Op, params: &[Val], out: &mut Val) -> Result<(), DsqgenError> {
    let (left, right) = (&params[0], &params[1]);
    match op {
        Op::Add => {
            if left.use_int && right.use_int {
                out.use_int = true;
                out.n = left.n.wrapping_add(right.n);
            } else {
                out.use_int = false;
                out.s.push_str(&left.render());
                out.s.push_str(&right.render());
            }
        }
        Op::Subtract => {
            out.use_int = true;
            out.n = left.n.wrapping_sub(right.n);
        }
        Op::Multiply => {
            out.use_int = true;
            out.n = left.n.wrapping_mul(right.n);
        }
        Op::Divide => {
            if right.n == 0 {
                return Err(evaluation("division by zero in a template expression"));
            }
            out.use_int = true;
            out.n = left.n.wrapping_div(right.n);
        }
    }
    Ok(())
}
