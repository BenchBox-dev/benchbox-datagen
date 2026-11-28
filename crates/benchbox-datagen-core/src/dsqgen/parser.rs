use std::cmp::Ordering;
use std::rc::Rc;

use super::DsqgenError;
use super::eval::Val;
use super::lexer::{Kw, Tok, Token, lex};
use crate::tpcds_update::REFRESH_SCALE_GRID;

pub(crate) const SUFFIX_LIST: u8 = 1;
pub(crate) const SUFFIX_ULIST: u8 = 2;
pub(crate) const SUFFIX_RANGE: u8 = 4;
const MAX_ARGS: usize = 20;
const GLOBAL_NAMES: [(&str, Kw); 8] = [
    ("_QUERY", Kw::Query),
    ("_STREAM", Kw::Stream),
    ("_TEMPLATE", Kw::Template),
    ("_SEED", Kw::Seed),
    ("_LIMIT", Kw::Limit),
    ("_LIMITA", Kw::LimitA),
    ("_LIMITB", Kw::LimitB),
    ("_LIMITC", Kw::LimitC),
];
const MAX_DEPTH: usize = 64;
const MAX_LIST: i64 = 10_000;
const MAX_USES: usize = 10_000;
const MAX_SUBSTITUTIONS: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Func {
    Date,
    Random,
    Dist,
    DistMember,
    DistWeight,
    Text,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug)]
pub(crate) enum ExprKind {
    Str(String),
    Int(i64),
    Keyword(Kw),
    Repl { text: String, weight: i64 },
    Subst { name: String, element: usize },
    Func { func: Func, args: Vec<Expr> },
    Arith { op: Op, args: Vec<Expr> },
}

#[derive(Debug)]
pub(crate) struct Expr {
    pub(crate) kind: ExprKind,
    pub(crate) suffix: u8,
    pub(crate) value_count: usize,
    pub(crate) perm_slot: usize,
    depth: usize,
}

impl Expr {
    fn new(kind: ExprKind) -> Self {
        let depth = match &kind {
            ExprKind::Func { args, .. } | ExprKind::Arith { args, .. } => {
                1 + args.iter().map(|arg| arg.depth).max().unwrap_or(0)
            }
            _ => 1,
        };
        Self {
            kind,
            suffix: 0,
            value_count: 1,
            perm_slot: 0,
            depth,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Sub {
    pub(crate) name: String,
    pub(crate) expr: Rc<Expr>,
    pub(crate) n_use: usize,
    pub(crate) n_parts: usize,
    pub(crate) vals: Vec<Val>,
    pub(crate) query_number: i64,
}

#[derive(Debug)]
pub(crate) struct Segment {
    pub(crate) text: String,
    pub(crate) sub: Option<usize>,
    pub(crate) count: usize,
    pub(crate) use_index: i64,
    pub(crate) end_of_statement: bool,
}

#[derive(Debug)]
pub(crate) struct Template {
    pub(crate) name: String,
    pub(crate) order: Vec<usize>,
    pub(crate) segments: Vec<Segment>,
    pub(crate) warnings: u32,
}

#[derive(Debug)]
pub(crate) struct Arena {
    pub(crate) subs: Vec<Sub>,
    pub(crate) limit_text: String,
    pub(crate) perm_slots: usize,
}

pub(crate) struct Environment<'a> {
    pub(crate) scale_factor: f64,
    pub(crate) rowcount: &'a dyn Fn(&str) -> Result<Option<i64>, DsqgenError>,
}

fn compare_names(left: &str, right: &str) -> Ordering {
    left.bytes()
        .map(|byte| byte.to_ascii_lowercase())
        .cmp(right.bytes().map(|byte| byte.to_ascii_lowercase()))
}

impl Arena {
    pub(crate) fn new() -> Self {
        let subs = GLOBAL_NAMES
            .iter()
            .map(|(name, kw)| Sub {
                name: (*name).to_owned(),
                expr: Rc::new(Expr::new(ExprKind::Keyword(*kw))),
                n_use: 0,
                n_parts: 1,
                vals: Vec::new(),
                query_number: 0,
            })
            .collect();
        Self {
            subs,
            limit_text: String::new(),
            perm_slots: 0,
        }
    }

    pub(crate) fn global(&self, name: &str) -> Option<usize> {
        GLOBAL_NAMES
            .iter()
            .position(|(known, _)| compare_names(known, name) == Ordering::Equal)
    }

    fn slot(&mut self) -> usize {
        self.perm_slots += 1;
        self.perm_slots - 1
    }
}

impl Template {
    fn add_sorted(&mut self, arena: &Arena, id: usize) {
        if self.order.is_empty() {
            self.order.push(id);
            return;
        }
        let head = self.order[0];
        if compare_names(&arena.subs[id].name, &arena.subs[head].name) != Ordering::Greater {
            self.order.insert(0, id);
        } else {
            self.order.insert(1, id);
        }
    }

    pub(crate) fn find_sub(&mut self, arena: &mut Arena, name: &str) -> Option<usize> {
        let stripped_len = name
            .find(|character: char| character.is_ascii_digit())
            .unwrap_or(name.len());
        let stripped = &name[..stripped_len];
        let found = self
            .order
            .iter()
            .copied()
            .find(|id| compare_names(&arena.subs[*id].name, stripped) == Ordering::Equal)
            .or_else(|| {
                let id = arena.global(stripped)?;
                self.add_sorted(arena, id);
                Some(id)
            })?;
        let digits: String = name[stripped_len..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        let uses = digits.parse::<usize>().unwrap_or(0).clamp(1, MAX_USES);
        if uses > arena.subs[found].n_use {
            arena.subs[found].n_use = uses;
        }
        Some(found)
    }

    fn define(&mut self, arena: &mut Arena, name: String, expr: Expr) {
        let id = arena.subs.len();
        arena.subs.push(Sub {
            name,
            n_parts: expr.value_count,
            expr: Rc::new(expr),
            n_use: 0,
            vals: Vec::new(),
            query_number: 0,
        });
        self.add_sorted(arena, id);
    }
}

struct Parser<'a> {
    tokens: Vec<Token>,
    at: usize,
    arena: &'a mut Arena,
    template: Template,
    env: &'a Environment<'a>,
    depth: usize,
}

fn syntax(line: u32, message: impl Into<String>) -> DsqgenError {
    DsqgenError::Template {
        line,
        message: message.into(),
    }
}

pub(crate) fn parse_template(
    arena: &mut Arena,
    env: &Environment<'_>,
    name: &str,
    template_text: &str,
    dialect_text: &str,
) -> Result<Template, DsqgenError> {
    let tokens = lex(&[dialect_text, template_text])?;
    let mut parser = Parser {
        tokens,
        at: 0,
        arena,
        template: Template {
            name: name.to_owned(),
            order: Vec::new(),
            segments: Vec::new(),
            warnings: 0,
        },
        env,
        depth: 0,
    };
    parser.workload()?;
    let Parser {
        arena,
        mut template,
        ..
    } = parser;
    let begin = template.find_sub(arena, "_BEGIN");
    template.segments.insert(
        0,
        Segment {
            text: "\n".to_owned(),
            sub: None,
            count: 0,
            use_index: 0,
            end_of_statement: false,
        },
    );
    template.segments.insert(
        0,
        Segment {
            text: String::new(),
            sub: begin,
            count: 0,
            use_index: 0,
            end_of_statement: false,
        },
    );
    Ok(template)
}

impl Parser<'_> {
    fn line(&self) -> u32 {
        self.tokens
            .get(self.at)
            .or_else(|| self.tokens.last())
            .map_or(0, |token| token.line)
    }

    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.at).map(|token| &token.tok)
    }

    fn next(&mut self) -> Result<Tok, DsqgenError> {
        let line = self.line();
        let token = self
            .tokens
            .get(self.at)
            .ok_or_else(|| syntax(line, "unexpected end of template"))?;
        self.at += 1;
        Ok(token.tok.clone())
    }

    fn eat(&mut self, expected: &Tok) -> bool {
        if self.peek() == Some(expected) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, expected: Tok) -> Result<(), DsqgenError> {
        let line = self.line();
        if self.eat(&expected) {
            Ok(())
        } else {
            Err(syntax(
                line,
                format!(
                    "syntax error: expected {expected:?}, found {:?}",
                    self.peek()
                ),
            ))
        }
    }

    fn expect_char(&mut self, character: char) -> Result<(), DsqgenError> {
        self.expect(Tok::Char(character))
    }

    fn expect_int(&mut self) -> Result<i64, DsqgenError> {
        let line = self.line();
        match self.next()? {
            Tok::Int(value) => Ok(value),
            other => Err(syntax(
                line,
                format!("syntax error: expected an integer, found {other:?}"),
            )),
        }
    }

    fn expect_literal(&mut self) -> Result<String, DsqgenError> {
        let line = self.line();
        match self.next()? {
            Tok::Literal(value) => Ok(value),
            other => Err(syntax(
                line,
                format!("syntax error: expected a literal, found {other:?}"),
            )),
        }
    }

    fn workload(&mut self) -> Result<(), DsqgenError> {
        if self.tokens.is_empty() {
            return Err(syntax(0, "the template has no statements"));
        }
        while self.peek().is_some() {
            self.statement()?;
        }
        self.add_segment("\n");
        self.add_substitution("_END", 0, 0)?;
        self.add_segment("\n");
        Ok(())
    }

    fn statement(&mut self) -> Result<(), DsqgenError> {
        let line = self.line();
        match self.peek() {
            Some(Tok::Char('#')) => Err(syntax(line, "#include statements are not supported")),
            Some(Tok::Kw(Kw::Dist)) => Err(syntax(line, "dist statements are not supported")),
            Some(Tok::Kw(Kw::Define)) => {
                self.at += 1;
                self.define_statement()
            }
            Some(Tok::Sql(_) | Tok::Char('[')) => self.query_statement(),
            other => Err(syntax(line, format!("syntax error near {other:?}"))),
        }
    }

    fn define_statement(&mut self) -> Result<(), DsqgenError> {
        let line = self.line();
        match self.next()? {
            Tok::Id(name) => {
                self.expect_char('=')?;
                if self.template.order.len() >= MAX_SUBSTITUTIONS {
                    return Err(syntax(line, "the template defines too many substitutions"));
                }
                let expr = self.expr()?;
                self.expect_char(';')?;
                self.template.define(self.arena, name, expr);
                Ok(())
            }
            Tok::Kw(Kw::Limit) => {
                self.expect_char('=')?;
                let value = self.expect_int()?;
                self.expect_char(';')?;
                self.template.find_sub(self.arena, "_LIMIT");
                self.arena.limit_text = value.to_string();
                Ok(())
            }
            other => Err(syntax(
                line,
                format!("syntax error after define: {other:?}"),
            )),
        }
    }

    fn add_segment(&mut self, text: &str) {
        self.template.segments.push(Segment {
            text: text.to_owned(),
            sub: None,
            count: 0,
            use_index: 0,
            end_of_statement: false,
        });
    }

    fn add_substitution(
        &mut self,
        name: &str,
        uses: usize,
        suffix: i64,
    ) -> Result<(), DsqgenError> {
        let line = self.line();
        let id = self.template.find_sub(self.arena, name).ok_or_else(|| {
            syntax(
                line,
                format!("substitution {name} is used before it is defined"),
            )
        })?;
        if self.template.segments.is_empty() {
            self.add_segment("");
        }
        if self
            .template
            .segments
            .last()
            .is_some_and(|segment| segment.sub.is_some())
        {
            self.add_segment("");
        }
        let listed = self.arena.subs[id].expr.suffix & (SUFFIX_LIST | SUFFIX_ULIST) != 0;
        let segment = self
            .template
            .segments
            .last_mut()
            .ok_or_else(|| syntax(line, "the template has no segments"))?;
        segment.sub = Some(id);
        segment.use_index = suffix.wrapping_sub(i64::from(listed));
        segment.count = uses;
        Ok(())
    }

    fn query_statement(&mut self) -> Result<(), DsqgenError> {
        loop {
            let line = self.line();
            match self.next()? {
                Tok::Sql(text) => self.add_segment(&text),
                Tok::Char('[') => self.substitution()?,
                Tok::Char(';') => {
                    if let Some(segment) = self.template.segments.last_mut() {
                        segment.end_of_statement = true;
                    }
                    return Ok(());
                }
                other => {
                    return Err(syntax(
                        line,
                        format!("syntax error in a query statement near {other:?}"),
                    ));
                }
            }
        }
    }

    fn suffix(&mut self) -> Result<i64, DsqgenError> {
        if !self.eat(&Tok::Char('.')) {
            return Ok(0);
        }
        let line = self.line();
        match self.next()? {
            Tok::Kw(Kw::Begin) => Ok(0),
            Tok::Kw(Kw::End) => Ok(1),
            Tok::Int(value) => Ok(value),
            other => Err(syntax(
                line,
                format!("syntax error in a substitution suffix near {other:?}"),
            )),
        }
    }

    fn substitution(&mut self) -> Result<(), DsqgenError> {
        let line = self.line();
        match self.next()? {
            Tok::Id(name) => {
                let uses = if let Some(Tok::Int(value)) = self.peek().cloned() {
                    self.at += 1;
                    value
                } else {
                    0
                };
                let suffix = self.suffix()?;
                self.expect_char(']')?;
                self.add_substitution(&name, usize::try_from(uses).unwrap_or(0), suffix)
            }
            Tok::Kw(
                kw @ (Kw::Query
                | Kw::Stream
                | Kw::Template
                | Kw::Seed
                | Kw::LimitA
                | Kw::LimitB
                | Kw::LimitC),
            ) => {
                self.expect_char(']')?;
                let name = GLOBAL_NAMES
                    .iter()
                    .find(|(_, known)| *known == kw)
                    .map_or("", |(name, _)| *name);
                self.add_substitution(name, 0, 0)
            }
            other => Err(syntax(
                line,
                format!("syntax error in a substitution near {other:?}"),
            )),
        }
    }

    fn expr(&mut self) -> Result<Expr, DsqgenError> {
        let mut left = self.term()?;
        loop {
            let op = match self.peek() {
                Some(Tok::Char('+')) => Op::Add,
                Some(Tok::Char('-')) => Op::Subtract,
                _ => return Ok(left),
            };
            self.at += 1;
            let line = self.line();
            let right = self.term()?;
            left = Self::arithmetic(line, op, left, right)?;
        }
    }

    fn term(&mut self) -> Result<Expr, DsqgenError> {
        let mut left = self.factor()?;
        loop {
            let op = match self.peek() {
                Some(Tok::Char('*')) => Op::Multiply,
                Some(Tok::Char('/')) => Op::Divide,
                _ => return Ok(left),
            };
            self.at += 1;
            let line = self.line();
            let right = self.factor()?;
            left = Self::arithmetic(line, op, left, right)?;
        }
    }

    fn arithmetic(line: u32, op: Op, left: Expr, right: Expr) -> Result<Expr, DsqgenError> {
        let expr = Expr::new(ExprKind::Arith {
            op,
            args: vec![left, right],
        });
        if expr.depth > MAX_DEPTH {
            return Err(syntax(line, "expressions nest too deeply"));
        }
        Ok(expr)
    }

    fn factor(&mut self) -> Result<Expr, DsqgenError> {
        let line = self.line();
        if self.depth >= MAX_DEPTH {
            return Err(syntax(line, "expressions nest too deeply"));
        }
        self.depth += 1;
        let outcome = self.factor_at_depth(line);
        self.depth -= 1;
        outcome
    }

    fn factor_at_depth(&mut self, line: u32) -> Result<Expr, DsqgenError> {
        match self.next()? {
            Tok::Literal(text) => Ok(Expr::new(ExprKind::Str(text))),
            Tok::Int(value) => Ok(Expr::new(ExprKind::Int(value))),
            Tok::Kw(kw @ (Kw::Uniform | Kw::Sales | Kw::Returns)) => {
                Ok(Expr::new(ExprKind::Keyword(kw)))
            }
            Tok::Kw(Kw::Scale) => Ok(Expr::new(ExprKind::Int(self.scale_integer()))),
            Tok::Char('[') => self.reference(),
            Tok::Kw(kw) => self.function(kw, line),
            other => Err(syntax(
                line,
                format!("syntax error in an expression near {other:?}"),
            )),
        }
    }

    fn scale_integer(&self) -> i64 {
        self.env.scale_factor as i64
    }

    fn reference(&mut self) -> Result<Expr, DsqgenError> {
        let line = self.line();
        match self.next()? {
            Tok::Kw(kw @ (Kw::Query | Kw::Template | Kw::Stream | Kw::Seed)) => {
                self.expect_char(']')?;
                Ok(Expr::new(ExprKind::Keyword(kw)))
            }
            Tok::Id(name) => {
                let element = if let Some(Tok::Int(value)) = self.peek().cloned() {
                    self.at += 1;
                    self.suffix()?;
                    value
                } else {
                    self.suffix()?
                };
                self.expect_char(']')?;
                if self.template.find_sub(self.arena, &name).is_none() {
                    self.template.warnings += 1;
                }
                Ok(Expr::new(ExprKind::Subst {
                    name,
                    element: usize::try_from(element).unwrap_or(0).max(1),
                }))
            }
            other => Err(syntax(
                line,
                format!("syntax error in a reference near {other:?}"),
            )),
        }
    }

    fn comma_list(&mut self, dist_style: bool) -> Result<Vec<Expr>, DsqgenError> {
        let mut args = Vec::new();
        loop {
            if dist_style && let Some(Tok::Id(name)) = self.peek().cloned() {
                self.at += 1;
                args.push(Expr::new(ExprKind::Str(name)));
            } else {
                args.push(self.expr()?);
            }
            if !self.eat(&Tok::Char(',')) {
                return Ok(args);
            }
        }
    }

    fn function_expr(line: u32, expr: Expr) -> Result<Expr, DsqgenError> {
        if expr.depth > MAX_DEPTH {
            return Err(syntax(line, "expressions nest too deeply"));
        }
        Ok(expr)
    }

    fn arg_count(line: u32, args: &[Expr], wanted: usize) -> Result<(), DsqgenError> {
        if args.len() == wanted {
            Ok(())
        } else {
            Err(syntax(
                line,
                format!("wanted {wanted} args, found {}", args.len()),
            ))
        }
    }

    fn function(&mut self, kw: Kw, line: u32) -> Result<Expr, DsqgenError> {
        match kw {
            Kw::Date | Kw::Random | Kw::Dist | Kw::DistMember | Kw::DistWeight => {
                let func = match kw {
                    Kw::Date => Func::Date,
                    Kw::Random => Func::Random,
                    Kw::Dist => Func::Dist,
                    Kw::DistMember => Func::DistMember,
                    _ => Func::DistWeight,
                };
                self.expect_char('(')?;
                let args = self.comma_list(!matches!(kw, Kw::Date | Kw::Random))?;
                self.expect_char(')')?;
                Self::arg_count(line, &args, 3)?;
                Self::function_expr(line, Expr::new(ExprKind::Func { func, args }))
            }
            Kw::Text => {
                self.expect_char('(')?;
                let mut args = Vec::new();
                loop {
                    self.expect_char('{')?;
                    let text = self.expect_literal()?;
                    self.expect_char(',')?;
                    let weight = self.expect_int()?;
                    self.expect_char('}')?;
                    args.push(Expr::new(ExprKind::Repl { text, weight }));
                    if !self.eat(&Tok::Char(',')) {
                        break;
                    }
                }
                self.expect_char(')')?;
                if args.len() > MAX_ARGS {
                    return Err(syntax(line, "too many arguments"));
                }
                Self::function_expr(
                    line,
                    Expr::new(ExprKind::Func {
                        func: Func::Text,
                        args,
                    }),
                )
            }
            Kw::Rowcount => self.rowcount(line),
            Kw::ScaleStep => {
                self.expect_char('(')?;
                self.expect_char(')')?;
                let target = self.scale_integer();
                let slot = REFRESH_SCALE_GRID
                    .iter()
                    .position(|volume| target <= i64::from(*volume));
                Ok(Expr::new(ExprKind::Int(
                    slot.map_or(9, |slot| slot as i64) + 1,
                )))
            }
            Kw::UList | Kw::List | Kw::Range => {
                self.expect_char('(')?;
                let mut expr = self.expr()?;
                self.expect_char(',')?;
                let count = self.expect_int()?;
                self.expect_char(')')?;
                let ExprKind::Func { args, .. } = &mut expr.kind else {
                    return Err(syntax(line, "list modifiers apply to function calls only"));
                };
                args.push(Expr::new(ExprKind::Int(count)));
                if !(0..=MAX_LIST).contains(&count) {
                    return Err(syntax(
                        line,
                        format!("a list size must be between 0 and {MAX_LIST}"),
                    ));
                }
                let count = count as usize;
                match kw {
                    Kw::UList => {
                        expr.value_count = count;
                        expr.suffix |= SUFFIX_ULIST;
                        expr.perm_slot = self.arena.slot();
                    }
                    Kw::List => {
                        expr.value_count = count;
                        expr.suffix |= SUFFIX_LIST;
                    }
                    _ => {
                        expr.value_count = 2;
                        expr.suffix |= SUFFIX_RANGE;
                    }
                }
                Ok(expr)
            }
            other => Err(syntax(
                line,
                format!("syntax error in an expression near {other:?}"),
            )),
        }
    }

    fn rowcount(&mut self, line: u32) -> Result<Expr, DsqgenError> {
        self.expect_char('(')?;
        let first = self.expect_literal()?;
        let second = if self.eat(&Tok::Char(',')) {
            Some(self.expect_literal()?)
        } else {
            None
        };
        self.expect_char(')')?;
        let lookup = |name: &str| -> Result<i64, DsqgenError> {
            (self.env.rowcount)(name)?.ok_or_else(|| {
                syntax(
                    line,
                    format!("{name:?} is not a table, pseudo table or distribution"),
                )
            })
        };
        let value = match &second {
            None => lookup(&first)?,
            Some(second) => lookup(&first)?.min(lookup(second)?),
        };
        Ok(Expr::new(ExprKind::Int(value)))
    }
}
