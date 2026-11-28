use super::DsqgenError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kw {
    Define,
    Random,
    Uniform,
    Range,
    Date,
    Include,
    Text,
    Dist,
    List,
    Rowcount,
    Begin,
    End,
    Sales,
    Returns,
    DistMember,
    DistWeight,
    Query,
    Stream,
    Template,
    Seed,
    Scale,
    ScaleStep,
    Set,
    Add,
    Names,
    Types,
    Weights,
    Int,
    Varchar,
    Decimal,
    Limit,
    LimitA,
    LimitB,
    LimitC,
    UList,
    Id,
    Sql,
}

const KEYWORDS: [(&str, Kw); 37] = [
    ("ID", Kw::Id),
    ("SQL", Kw::Sql),
    ("DEFINE", Kw::Define),
    ("RANDOM", Kw::Random),
    ("UNIFORM", Kw::Uniform),
    ("RANGE", Kw::Range),
    ("DATE", Kw::Date),
    ("INCLUDE", Kw::Include),
    ("TEXT", Kw::Text),
    ("DIST", Kw::Dist),
    ("LIST", Kw::List),
    ("ROWCOUNT", Kw::Rowcount),
    ("BEGIN", Kw::Begin),
    ("END", Kw::End),
    ("SALES", Kw::Sales),
    ("RETURNS", Kw::Returns),
    ("DISTMEMBER", Kw::DistMember),
    ("DISTWEIGHT", Kw::DistWeight),
    ("_QUERY", Kw::Query),
    ("_STREAM", Kw::Stream),
    ("_TEMPLATE", Kw::Template),
    ("_SEED", Kw::Seed),
    ("SCALE", Kw::Scale),
    ("SCALESTEP", Kw::ScaleStep),
    ("SET", Kw::Set),
    ("ADD", Kw::Add),
    ("NAMES", Kw::Names),
    ("TYPES", Kw::Types),
    ("WEIGHTS", Kw::Weights),
    ("INT", Kw::Int),
    ("VARCHAR", Kw::Varchar),
    ("DECIMAL", Kw::Decimal),
    ("_LIMIT", Kw::Limit),
    ("_LIMITA", Kw::LimitA),
    ("_LIMITB", Kw::LimitB),
    ("_LIMITC", Kw::LimitC),
    ("ULIST", Kw::UList),
];

pub(crate) fn keyword(word: &str) -> Option<Kw> {
    KEYWORDS
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(word))
        .map(|(_, kw)| *kw)
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Tok {
    Int(i64),
    Decimal,
    Literal(String),
    Id(String),
    Sql(String),
    Kw(Kw),
    Char(char),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Normal,
    Sql,
    Subst,
}

pub(crate) struct Token {
    pub(crate) tok: Tok,
    pub(crate) line: u32,
}

fn is_id_start(byte: u8) -> bool {
    byte == b'_' || byte.is_ascii_alphabetic()
}

fn is_id_part(byte: u8) -> bool {
    byte == b'_' || byte.is_ascii_alphanumeric()
}

fn digits_at(bytes: &[u8], from: usize) -> usize {
    bytes[from.min(bytes.len())..]
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count()
}

fn error(line: u32, message: impl Into<String>) -> DsqgenError {
    DsqgenError::Template {
        line,
        message: message.into(),
    }
}

pub(crate) fn lex(chunks: &[&str]) -> Result<Vec<Token>, DsqgenError> {
    let mut tokens = Vec::new();
    let mut state = State::Normal;
    let mut old_state = State::Normal;
    let mut first_word = true;
    for text in chunks {
        let bytes = text.as_bytes();
        let mut at = 0;
        let mut line = 1_u32;
        while at < bytes.len() {
            let byte = bytes[at];
            match state {
                State::Normal => match byte {
                    b' ' | b'\t' => at += 1,
                    b'-' if bytes.get(at + 1) == Some(&b'-') => {
                        while at < bytes.len() && bytes[at] != b'\n' {
                            at += 1;
                        }
                    }
                    b'\n' => {
                        line += 1;
                        first_word = true;
                        at += 1;
                    }
                    b'[' => {
                        old_state = State::Normal;
                        state = State::Subst;
                        tokens.push(Token {
                            tok: Tok::Char('['),
                            line,
                        });
                        at += 1;
                    }
                    b'"' => {
                        let close = bytes[at + 1..]
                            .iter()
                            .position(|candidate| *candidate == b'"' || *candidate == b'\n');
                        match close {
                            Some(offset) if bytes[at + 1 + offset] == b'"' => {
                                tokens.push(Token {
                                    tok: Tok::Literal(text[at + 1..at + 1 + offset].to_owned()),
                                    line,
                                });
                                first_word = false;
                                at += offset + 2;
                            }
                            _ => {
                                first_word = false;
                                tokens.push(Token {
                                    tok: Tok::Char('"'),
                                    line,
                                });
                                at += 1;
                            }
                        }
                    }
                    _ if byte.is_ascii_digit()
                        || (byte == b'-' && digits_at(bytes, at + 1) > 0)
                        || (byte == b'.' && digits_at(bytes, at + 1) > 0) =>
                    {
                        let start = at;
                        if bytes[at] == b'-' {
                            at += 1;
                        }
                        at += digits_at(bytes, at);
                        first_word = false;
                        if bytes.get(at) == Some(&b'.') && digits_at(bytes, at + 1) > 0 {
                            at += 1 + digits_at(bytes, at + 1);
                            tokens.push(Token {
                                tok: Tok::Decimal,
                                line,
                            });
                        } else {
                            let value = text[start..at]
                                .parse::<i64>()
                                .map_err(|_| error(line, "integer out of range"))?;
                            tokens.push(Token {
                                tok: Tok::Int(value),
                                line,
                            });
                        }
                    }
                    _ if is_id_start(byte) => {
                        let start = at;
                        while at < bytes.len() && is_id_part(bytes[at]) {
                            at += 1;
                        }
                        let word = &text[start..at];
                        if let Some(kw) = keyword(word) {
                            first_word = false;
                            tokens.push(Token {
                                tok: Tok::Kw(kw),
                                line,
                            });
                        } else if first_word {
                            state = State::Sql;
                            tokens.push(Token {
                                tok: Tok::Sql(word.to_owned()),
                                line,
                            });
                        } else {
                            tokens.push(Token {
                                tok: Tok::Id(word.to_owned()),
                                line,
                            });
                        }
                    }
                    _ => {
                        let character = text[at..].chars().next().unwrap_or('\u{fffd}');
                        first_word = false;
                        tokens.push(Token {
                            tok: Tok::Char(character),
                            line,
                        });
                        at += character.len_utf8();
                    }
                },
                State::Sql => match byte {
                    b';' => {
                        state = State::Normal;
                        tokens.push(Token {
                            tok: Tok::Char(';'),
                            line,
                        });
                        at += 1;
                    }
                    b'[' => {
                        old_state = State::Sql;
                        state = State::Subst;
                        tokens.push(Token {
                            tok: Tok::Char('['),
                            line,
                        });
                        at += 1;
                    }
                    b']' => {
                        tokens.push(Token {
                            tok: Tok::Char(']'),
                            line,
                        });
                        at += 1;
                    }
                    _ => {
                        let start = at;
                        while at < bytes.len() && !matches!(bytes[at], b';' | b'[' | b']' | b'\n') {
                            at += 1;
                        }
                        if bytes.get(at) == Some(&b'\n') {
                            at += 1;
                            tokens.push(Token {
                                tok: Tok::Sql(text[start..at].to_owned()),
                                line,
                            });
                            line += 1;
                        } else {
                            tokens.push(Token {
                                tok: Tok::Sql(text[start..at].to_owned()),
                                line,
                            });
                        }
                    }
                },
                State::Subst => match byte {
                    b']' => {
                        state = old_state;
                        tokens.push(Token {
                            tok: Tok::Char(']'),
                            line,
                        });
                        at += 1;
                    }
                    b'.' => {
                        tokens.push(Token {
                            tok: Tok::Char('.'),
                            line,
                        });
                        at += 1;
                    }
                    _ if byte.is_ascii_digit()
                        || (byte == b'-' && digits_at(bytes, at + 1) > 0) =>
                    {
                        let start = at;
                        if bytes[at] == b'-' {
                            at += 1;
                        }
                        at += digits_at(bytes, at);
                        let value = text[start..at]
                            .parse::<i64>()
                            .map_err(|_| error(line, "integer out of range"))?;
                        tokens.push(Token {
                            tok: Tok::Int(value),
                            line,
                        });
                    }
                    _ if byte == b'_' || byte.is_ascii_alphabetic() => {
                        let start = at;
                        while at < bytes.len()
                            && (bytes[at] == b'_' || bytes[at].is_ascii_alphabetic())
                        {
                            at += 1;
                        }
                        let word = &text[start..at];
                        match keyword(word) {
                            Some(kw) => tokens.push(Token {
                                tok: Tok::Kw(kw),
                                line,
                            }),
                            None => tokens.push(Token {
                                tok: Tok::Id(word.to_owned()),
                                line,
                            }),
                        }
                    }
                    _ => return Err(error(line, "unexpected character inside a substitution")),
                },
            }
        }
    }
    Ok(tokens)
}
