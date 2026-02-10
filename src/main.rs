#![allow(warnings)]
// relay.rs — Relay v0.1 single-file interpreter (Tokio runtime)
// - Indentation-based parser (spaces only, 4 per level)
// - AST + async evaluator
// - Implicit async resolution via Value::Deferred (auto-resolve in value contexts)
// - Stdlib: timers, fs/json, str/int/float, http client, web server + routing (Axum)
// Build:
//   cargo new relay && replace src/main.rs with this file contents (or use as relay.rs)
//   Cargo.toml deps: tokio, thiserror, serde, serde_json, reqwest, axum, tower, tower-http, html-escape, indexmap, mongodb, futures
// Run:
//   cargo run -- path/to/app.ry

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    path::Component,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    extract::{Path, Query},
    http::{HeaderMap, Method, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json as AxumJson, Router,
};

use async_recursion::async_recursion;
use futures::stream::TryStreamExt;
use html_escape::encode_safe;
use indexmap::IndexMap;
use mongodb::{
    bson::{self, Bson, Document},
    Client as MongoClient, Collection as MongoCollection, Database as MongoDatabase,
};
use serde_json::Value as J;
use std::sync::OnceLock;
use thiserror::Error;

// ========================= Errors =========================

#[derive(Debug, Error)]
pub enum RelayError {
    #[error("Syntax error: {msg} at line {line}, col {col}")]
    Syntax {
        msg: String,
        line: usize,
        col: usize,
    },

    #[error("Runtime error: {0}")]
    Runtime(String),

    #[error("Name error: {0}")]
    Name(String),

    #[error("Type error: {0}")]
    Type(String),
}
type RResult<T> = Result<T, RelayError>;

// ========================= AST =========================

#[derive(Debug, Clone)]
struct Program {
    stmts: Vec<Stmt>,
}

#[derive(Debug, Clone)]
enum Stmt {
    Expr(Expr),
    Import {
        module: String,
    },
    Assign {
        name: String,
        expr: Expr,
    },
    AugAssign {
        name: String,
        op: AugOp,
        expr: Expr,
    },
    DestructureAssign {
        targets: Vec<String>,
        expr: Expr,
    },
    Return(Option<Expr>),

    If {
        cond: Expr,
        then_block: Vec<Stmt>,
        else_block: Vec<Stmt>,
    },
    While {
        cond: Expr,
        body: Vec<Stmt>,
    },
    For {
        var: String,
        iter: Expr,
        body: Vec<Stmt>,
    },
    TryExcept {
        err_name: Option<String>,
        try_block: Vec<Stmt>,
        except_block: Vec<Stmt>,
    },

    FuncDef {
        name: String,
        params: Vec<Param>,
        body: Vec<Stmt>,
        decorators: Vec<Decorator>,
    },
}

#[derive(Debug, Clone, Copy)]
enum AugOp {
    Add,
    Sub,
}

#[derive(Debug, Clone)]
struct Param {
    name: String,
    ty: Option<String>, // "str", "int", "Json" ...
    default: Option<Expr>,
}

#[derive(Debug, Clone)]
enum Decorator {
    // @route.get("/x") or @myroute.post("/x")
    Route {
        base: String, // variable name holding Route handle
        method: String,
        path: String,
    },
}

#[derive(Debug, Clone)]
enum Expr {
    Int(i64),
    Float(f64),
    Bool(bool),
    None,
    Str(String),
    Ident(String),

    List(Vec<Expr>),
    ListComp {
        expr: Box<Expr>,
        var: String,
        iter: Box<Expr>,
        filter: Option<Box<Expr>>,
    },
    Dict(Vec<(Expr, Expr)>),

    Member {
        object: Box<Expr>,
        name: String,
    }, // obj.name
    Call {
        callee: Box<Expr>,
        args: Vec<Expr>,
        kwargs: Vec<(String, Expr)>,
    },
    Index {
        target: Box<Expr>,
        index: Box<Expr>,
    },

    BinOp {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
}

#[derive(Debug, Clone, Copy)]
enum UnaryOp {
    Neg,
    Not,
}

#[derive(Debug, Clone, Copy)]
enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

// ========================= Lexer =========================

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Int(i64),
    Float(f64),
    Str(String),

    Newline,
    Indent,
    Dedent,

    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Comma,
    Dot,
    At,

    Assign, // =
    AugAdd, // =+
    AugSub, // =-
    Colon,  // used in dict literals only
    Op(String),

    Keyword(String), // fn if for while return try except import True False None str int float
}

#[derive(Debug, Clone)]
struct Token {
    kind: Tok,
    line: usize,
    col: usize,
}

struct Lexer<'a> {
    s: &'a str,
    i: usize,
    line: usize,
    col: usize,
    indent_stack: Vec<usize>,
    pending_dedents: usize,
    at_line_start: bool,
}

impl<'a> Lexer<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            s,
            i: 0,
            line: 1,
            col: 1,
            indent_stack: vec![0],
            pending_dedents: 0,
            at_line_start: true,
        }
    }

    fn tokenize(&mut self) -> RResult<Vec<Token>> {
        let mut out = Vec::new();

        while self.i < self.s.len() || self.pending_dedents > 0 {
            if self.pending_dedents > 0 {
                self.pending_dedents -= 1;
                out.push(self.tok(Tok::Dedent));
                continue;
            }
            if self.i >= self.s.len() {
                break;
            }

            // indentation
            if self.at_line_start {
                let (spaces, has_tabs) = self.count_leading_ws();
                if has_tabs {
                    return Err(self.err("Tabs are not allowed (spaces only)"));
                }
                // blank line or comment-only line => ignore indentation
                if self.peek_is_newline() || self.peek_is_comment_start() {
                    // no indent changes
                } else {
                    if spaces % 4 != 0 {
                        return Err(self.err("Indentation must be 4 spaces per level"));
                    }
                    let cur = *self.indent_stack.last().unwrap();
                    if spaces > cur {
                        self.indent_stack.push(spaces);
                        out.push(self.tok(Tok::Indent));
                    } else if spaces < cur {
                        while *self.indent_stack.last().unwrap() > spaces {
                            self.indent_stack.pop();
                            self.pending_dedents += 1;
                        }
                        if *self.indent_stack.last().unwrap() != spaces {
                            return Err(self.err("Indentation mismatch"));
                        }
                        if self.pending_dedents > 0 {
                            continue;
                        }
                    }
                }
                self.at_line_start = false;
            }

            self.skip_ws_inline();
            if self.i >= self.s.len() {
                break;
            }

            // comments
            if self.peek2("//") {
                self.consume_until_newline();
                continue;
            }
            if self.peek2("/*") {
                self.consume_block_comment()?;
                continue;
            }

            let c = self.peek_char().unwrap();
            if c == '\n' {
                self.advance_char();
                out.push(self.tok(Tok::Newline));
                self.at_line_start = true;
                continue;
            }

            let kind = match c {
                '(' => {
                    self.advance_char();
                    Tok::LParen
                }
                ')' => {
                    self.advance_char();
                    Tok::RParen
                }
                '[' => {
                    self.advance_char();
                    Tok::LBracket
                }
                ']' => {
                    self.advance_char();
                    Tok::RBracket
                }
                '{' => {
                    self.advance_char();
                    Tok::LBrace
                }
                '}' => {
                    self.advance_char();
                    Tok::RBrace
                }
                ',' => {
                    self.advance_char();
                    Tok::Comma
                }
                '.' => {
                    self.advance_char();
                    Tok::Dot
                }
                '@' => {
                    self.advance_char();
                    Tok::At
                }
                ':' => {
                    self.advance_char();
                    Tok::Colon
                }
                '"' => Tok::Str(self.read_string()?),
                '=' => {
                    if self.peek2("=+") {
                        self.advance_n(2);
                        Tok::AugAdd
                    } else if self.peek2("=-") {
                        self.advance_n(2);
                        Tok::AugSub
                    } else if self.peek2("==") {
                        self.advance_n(2);
                        Tok::Op("==".into())
                    } else {
                        self.advance_char();
                        Tok::Assign
                    }
                }
                '!' => {
                    if self.peek2("!=") {
                        self.advance_n(2);
                        Tok::Op("!=".into())
                    } else {
                        return Err(self.err("Unexpected '!'"));
                    }
                }
                '<' => {
                    if self.peek2("<=") {
                        self.advance_n(2);
                        Tok::Op("<=".into())
                    } else {
                        self.advance_char();
                        Tok::Op("<".into())
                    }
                }
                '>' => {
                    if self.peek2(">=") {
                        self.advance_n(2);
                        Tok::Op(">=".into())
                    } else {
                        self.advance_char();
                        Tok::Op(">".into())
                    }
                }
                '+' | '-' | '*' | '/' => {
                    self.advance_char();
                    Tok::Op(c.to_string())
                }
                _ => {
                    if c.is_ascii_digit() {
                        let t = self.read_number()?;
                        out.push(self.wrap(t));
                        continue;
                    }
                    if is_ident_start(c) {
                        let id = self.read_ident();
                        let k = match id.as_str() {
                            "fn" | "if" | "for" | "while" | "return" | "try" | "except"
                            | "import" | "True" | "False" | "None" | "str" | "int" | "float" => {
                                Tok::Keyword(id)
                            }
                            _ => Tok::Ident(id),
                        };
                        out.push(self.wrap(k));
                        continue;
                    }
                    return Err(self.err(&format!("Unexpected char: {c}")));
                }
            };

            out.push(self.wrap(kind));
        }

        while self.indent_stack.len() > 1 {
            self.indent_stack.pop();
            out.push(self.tok(Tok::Dedent));
        }

        Ok(out)
    }

    fn tok(&self, kind: Tok) -> Token {
        Token {
            kind,
            line: self.line,
            col: self.col,
        }
    }
    fn wrap(&self, kind: Tok) -> Token {
        self.tok(kind)
    }
    fn err(&self, msg: &str) -> RelayError {
        RelayError::Syntax {
            msg: msg.into(),
            line: self.line,
            col: self.col,
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.s[self.i..].chars().next()
    }
    fn advance_char(&mut self) {
        let c = self.peek_char().unwrap();
        self.i += c.len_utf8();
        if c == '\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
    }
    fn advance_n(&mut self, n: usize) {
        for _ in 0..n {
            self.advance_char();
        }
    }
    fn peek2(&self, lit: &str) -> bool {
        self.s[self.i..].starts_with(lit)
    }
    fn peek_is_newline(&self) -> bool {
        self.s[self.i..].starts_with('\n')
    }
    fn peek_is_comment_start(&self) -> bool {
        self.peek2("//") || self.peek2("/*")
    }

    fn count_leading_ws(&mut self) -> (usize, bool) {
        let mut j = self.i;
        let mut spaces = 0usize;
        let mut has_tabs = false;
        while j < self.s.len() {
            let c = self.s[j..].chars().next().unwrap();
            if c == ' ' {
                spaces += 1;
                j += 1;
            } else if c == '\t' {
                has_tabs = true;
                j += 1;
            } else {
                break;
            }
        }
        while self.i < j {
            self.advance_char();
        }
        (spaces, has_tabs)
    }

    fn skip_ws_inline(&mut self) {
        while let Some(c) = self.peek_char() {
            if c == ' ' || c == '\r' {
                self.advance_char();
            } else {
                break;
            }
        }
    }

    fn consume_until_newline(&mut self) {
        while let Some(c) = self.peek_char() {
            if c == '\n' {
                break;
            }
            self.advance_char();
        }
    }

    fn consume_block_comment(&mut self) -> RResult<()> {
        self.advance_n(2); // /*
        while self.i < self.s.len() {
            if self.peek2("*/") {
                self.advance_n(2);
                return Ok(());
            }
            self.advance_char();
        }
        Err(self.err("Unterminated block comment"))
    }

    fn read_string(&mut self) -> RResult<String> {
        self.advance_char(); // opening "
        let mut out = String::new();
        while let Some(c) = self.peek_char() {
            if c == '"' {
                self.advance_char();
                return Ok(out);
            }
            if c == '\\' {
                self.advance_char();
                let esc = self
                    .peek_char()
                    .ok_or_else(|| self.err("Bad string escape"))?;
                self.advance_char();
                out.push(match esc {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    '"' => '"',
                    '\\' => '\\',
                    other => other,
                });
                continue;
            }
            self.advance_char();
            out.push(c);
        }
        Err(self.err("Unterminated string"))
    }

    fn read_number(&mut self) -> RResult<Tok> {
        let start = self.i;
        let mut saw_dot = false;
        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() {
                self.advance_char();
            } else if c == '.' && !saw_dot {
                saw_dot = true;
                self.advance_char();
            } else {
                break;
            }
        }
        let s = &self.s[start..self.i];
        if saw_dot {
            let f: f64 = s.parse().map_err(|_| self.err("Bad float"))?;
            Ok(Tok::Float(f))
        } else {
            let n: i64 = s.parse().map_err(|_| self.err("Bad int"))?;
            Ok(Tok::Int(n))
        }
    }

    fn read_ident(&mut self) -> String {
        let start = self.i;
        self.advance_char();
        while let Some(c) = self.peek_char() {
            if is_ident_continue(c) {
                self.advance_char();
            } else {
                break;
            }
        }
        self.s[start..self.i].to_string()
    }
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}
fn is_ident_continue(c: char) -> bool {
    is_ident_start(c) || c.is_ascii_digit()
}

// ========================= Parser =========================

struct Parser {
    t: Vec<Token>,
    i: usize,
}

impl Parser {
    fn new(t: Vec<Token>) -> Self {
        Self { t, i: 0 }
    }

    fn parse_program(&mut self) -> RResult<Program> {
        let mut stmts = Vec::new();
        self.skip_newlines();
        while !self.eof() {
            if self.peek_is(&Tok::Dedent) {
                self.bump();
                continue;
            }
            stmts.push(self.parse_stmt()?);
            self.skip_newlines();
        }
        Ok(Program { stmts })
    }

    fn parse_stmt(&mut self) -> RResult<Stmt> {
        let mut decorators = Vec::new();
        while self.peek_is(&Tok::At) {
            decorators.push(self.parse_decorator()?);
            self.expect_newline()?;
            self.skip_newlines();
        }

        if self.peek_kw("fn") {
            return self.parse_func_def(decorators);
        }
        if self.peek_kw("if") {
            return self.parse_if();
        }
        if self.peek_kw("while") {
            return self.parse_while();
        }
        if self.peek_kw("for") {
            return self.parse_for();
        }
        if self.peek_kw("return") {
            return self.parse_return();
        }
        if self.peek_kw("try") {
            return self.parse_try_except();
        }
        if self.peek_kw("import") {
            return self.parse_import();
        }

        if self.peek_destructure_assign() {
            return self.parse_destructure_assign();
        }

        // assignment
        if let Some(name) = self.peek_ident_string() {
            if self.peek_n_is(1, &Tok::Assign)
                || self.peek_n_is(1, &Tok::AugAdd)
                || self.peek_n_is(1, &Tok::AugSub)
            {
                self.bump(); // ident
                let op = self.bump().kind.clone();
                let expr = self.parse_expr()?;
                return Ok(match op {
                    Tok::Assign => Stmt::Assign { name, expr },
                    Tok::AugAdd => Stmt::AugAssign {
                        name,
                        op: AugOp::Add,
                        expr,
                    },
                    Tok::AugSub => Stmt::AugAssign {
                        name,
                        op: AugOp::Sub,
                        expr,
                    },
                    _ => unreachable!(),
                });
            }
        }

        Ok(Stmt::Expr(self.parse_expr()?))
    }

    fn parse_decorator(&mut self) -> RResult<Decorator> {
        self.expect(&Tok::At)?;
        let base = self.expect_ident()?; // route var name
        self.expect(&Tok::Dot)?;
        let method = self.expect_ident()?;
        self.expect(&Tok::LParen)?;
        let path = match self.bump().kind.clone() {
            Tok::Str(s) => s,
            _ => return Err(self.err_here("Decorator expects string path")),
        };
        self.expect(&Tok::RParen)?;
        Ok(Decorator::Route { base, method, path })
    }

    fn parse_func_def(&mut self, decorators: Vec<Decorator>) -> RResult<Stmt> {
        self.expect_kw("fn")?;
        let name = self.expect_ident()?;
        self.expect(&Tok::LParen)?;
        let mut params = Vec::new();
        if !self.peek_is(&Tok::RParen) {
            loop {
                let pname = self.expect_ident()?;
                let mut ty = None;
                let mut default = None;

                if self.peek_is(&Tok::Colon) {
                    self.bump();
                    ty = Some(self.expect_ident()?);
                }
                if self.peek_is(&Tok::Assign) {
                    self.bump();
                    default = Some(self.parse_expr()?);
                }

                params.push(Param {
                    name: pname,
                    ty,
                    default,
                });

                if self.peek_is(&Tok::Comma) {
                    self.bump();
                    continue;
                }
                break;
            }
        }
        self.expect(&Tok::RParen)?;
        self.expect_newline()?;
        let body = self.parse_block()?;
        Ok(Stmt::FuncDef {
            name,
            params,
            body,
            decorators,
        })
    }

    fn parse_if(&mut self) -> RResult<Stmt> {
        self.expect_kw("if")?;
        self.expect(&Tok::LParen)?;
        let cond = self.parse_expr()?;
        self.expect(&Tok::RParen)?;
        self.expect_newline()?;
        let then_block = self.parse_block()?;
        let mut else_block = Vec::new();
        self.skip_newlines();
        if self.peek_ident_string().as_deref() == Some("else") {
            self.bump();
            self.expect_newline()?;
            else_block = self.parse_block()?;
        }
        Ok(Stmt::If {
            cond,
            then_block,
            else_block,
        })
    }

    fn parse_while(&mut self) -> RResult<Stmt> {
        self.expect_kw("while")?;
        self.expect(&Tok::LParen)?;
        let cond = self.parse_expr()?;
        self.expect(&Tok::RParen)?;
        self.expect_newline()?;
        let body = self.parse_block()?;
        Ok(Stmt::While { cond, body })
    }

    fn parse_for(&mut self) -> RResult<Stmt> {
        self.expect_kw("for")?;
        self.expect(&Tok::LParen)?;
        let var = self.expect_ident()?;
        let kw_in = self.expect_ident()?;
        if kw_in != "in" {
            return Err(self.err_here("Expected 'in' in for-loop"));
        }
        let iter = self.parse_expr()?;
        self.expect(&Tok::RParen)?;
        self.expect_newline()?;
        let body = self.parse_block()?;
        Ok(Stmt::For { var, iter, body })
    }

    fn parse_return(&mut self) -> RResult<Stmt> {
        self.expect_kw("return")?;
        if self.peek_is(&Tok::Newline) || self.peek_is(&Tok::Dedent) {
            return Ok(Stmt::Return(None));
        }
        Ok(Stmt::Return(Some(self.parse_expr()?)))
    }

    fn parse_try_except(&mut self) -> RResult<Stmt> {
        self.expect_kw("try")?;
        self.expect_newline()?;
        let try_block = self.parse_block()?;
        self.skip_newlines();

        self.expect_kw("except")?;
        let err_name = if self.peek_is(&Tok::LParen) {
            self.bump();
            let name = self.expect_ident()?;
            self.expect(&Tok::RParen)?;
            Some(name)
        } else {
            None
        };
        self.expect_newline()?;
        let except_block = self.parse_block()?;

        Ok(Stmt::TryExcept {
            err_name,
            try_block,
            except_block,
        })
    }

    fn parse_import(&mut self) -> RResult<Stmt> {
        self.expect_kw("import")?;
        let mut module = self.expect_ident()?;
        while self.peek_is(&Tok::Dot) {
            self.bump();
            module.push('.');
            module.push_str(&self.expect_ident()?);
        }
        Ok(Stmt::Import { module })
    }

    fn peek_destructure_assign(&self) -> bool {
        let mut idx = 0usize;
        if !matches!(
            self.t.get(self.i + idx).map(|t| &t.kind),
            Some(Tok::Ident(_))
        ) {
            return false;
        }
        idx += 1;
        let mut count = 1usize;
        while matches!(self.t.get(self.i + idx).map(|t| &t.kind), Some(Tok::Comma))
            && matches!(
                self.t.get(self.i + idx + 1).map(|t| &t.kind),
                Some(Tok::Ident(_))
            )
        {
            idx += 2;
            count += 1;
        }
        count > 1 && matches!(self.t.get(self.i + idx).map(|t| &t.kind), Some(Tok::Assign))
    }

    fn parse_destructure_assign(&mut self) -> RResult<Stmt> {
        let mut targets = vec![self.expect_ident()?];
        while self.peek_is(&Tok::Comma) {
            self.bump();
            targets.push(self.expect_ident()?);
        }
        self.expect(&Tok::Assign)?;
        let expr = self.parse_expr()?;
        Ok(Stmt::DestructureAssign { targets, expr })
    }

    fn parse_block(&mut self) -> RResult<Vec<Stmt>> {
        self.expect(&Tok::Indent)?;
        self.skip_newlines();
        let mut out = Vec::new();
        while !self.eof() && !self.peek_is(&Tok::Dedent) {
            out.push(self.parse_stmt()?);
            self.skip_newlines();
        }
        self.expect(&Tok::Dedent)?;
        Ok(out)
    }

    // ---------- expressions ----------

    fn parse_expr(&mut self) -> RResult<Expr> {
        self.parse_binop(0)
    }

    fn parse_binop(&mut self, min_prec: u8) -> RResult<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            let (op, prec) = match self.peek_binop() {
                Some(v) => v,
                None => break,
            };
            if prec < min_prec {
                break;
            }
            self.bump(); // op
            let right = self.parse_binop(prec + 1)?;
            left = Expr::BinOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> RResult<Expr> {
        if self.peek_op("-") {
            self.bump();
            let e = self.parse_unary()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(e),
            });
        }
        if self.peek_ident_string().as_deref() == Some("not") {
            self.bump();
            let e = self.parse_unary()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(e),
            });
        }
        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> RResult<Expr> {
        let mut e = self.parse_primary()?;

        loop {
            if self.peek_is(&Tok::Dot) {
                self.bump();
                let name = self.expect_ident()?;
                e = Expr::Member {
                    object: Box::new(e),
                    name,
                };
                continue;
            }

            if self.peek_is(&Tok::LParen) {
                self.bump();
                let mut args = Vec::new();
                let mut kwargs = Vec::new();
                if !self.peek_is(&Tok::RParen) {
                    loop {
                        // kwarg: ident = expr
                        if let Some(k) = self.peek_ident_string() {
                            if self.peek_n_is(1, &Tok::Assign) {
                                self.bump();
                                self.bump(); // =
                                let v = self.parse_expr()?;
                                kwargs.push((k, v));
                            } else {
                                args.push(self.parse_expr()?);
                            }
                        } else {
                            args.push(self.parse_expr()?);
                        }
                        if self.peek_is(&Tok::Comma) {
                            self.bump();
                            continue;
                        }
                        break;
                    }
                }
                self.expect(&Tok::RParen)?;
                e = Expr::Call {
                    callee: Box::new(e),
                    args,
                    kwargs,
                };
                continue;
            }

            if self.peek_is(&Tok::LBracket) {
                self.bump();
                let idx = self.parse_expr()?;
                self.expect(&Tok::RBracket)?;
                e = Expr::Index {
                    target: Box::new(e),
                    index: Box::new(idx),
                };
                continue;
            }

            break;
        }

        Ok(e)
    }

    fn parse_primary(&mut self) -> RResult<Expr> {
        let tok = self.bump().clone();
        match tok.kind {
            Tok::Int(n) => Ok(Expr::Int(n)),
            Tok::Float(f) => Ok(Expr::Float(f)),
            Tok::Str(s) => Ok(Expr::Str(s)),
            Tok::Ident(s) => Ok(Expr::Ident(s)),
            Tok::Keyword(k) => match k.as_str() {
                "True" => Ok(Expr::Bool(true)),
                "False" => Ok(Expr::Bool(false)),
                "None" => Ok(Expr::None),
                "str" | "int" | "float" => Ok(Expr::Ident(k)),
                _ => Err(self.err_at(tok.line, tok.col, "Unexpected keyword in expression")),
            },
            Tok::LParen => {
                let e = self.parse_expr()?;
                self.expect(&Tok::RParen)?;
                Ok(e)
            }
            Tok::LBracket => {
                let mut items = Vec::new();
                if !self.peek_is(&Tok::RBracket) {
                    let first = self.parse_expr()?;
                    if self.peek_kw("for") {
                        self.bump();
                        let var = self.expect_ident()?;
                        let kw_in = self.expect_ident()?;
                        if kw_in != "in" {
                            return Err(self.err_here("Expected 'in' in list comprehension"));
                        }
                        let iter = self.parse_expr()?;
                        let filter = if self.peek_kw("if") {
                            self.bump();
                            Some(Box::new(self.parse_expr()?))
                        } else {
                            None
                        };
                        self.expect(&Tok::RBracket)?;
                        return Ok(Expr::ListComp {
                            expr: Box::new(first),
                            var,
                            iter: Box::new(iter),
                            filter,
                        });
                    }

                    items.push(first);
                    while self.peek_is(&Tok::Comma) {
                        self.bump();
                        items.push(self.parse_expr()?);
                    }
                }
                self.expect(&Tok::RBracket)?;
                Ok(Expr::List(items))
            }
            Tok::LBrace => {
                let mut kvs = Vec::new();
                if !self.peek_is(&Tok::RBrace) {
                    loop {
                        let k = self.parse_expr()?;
                        self.expect(&Tok::Colon)?;
                        let v = self.parse_expr()?;
                        kvs.push((k, v));
                        if self.peek_is(&Tok::Comma) {
                            self.bump();
                            continue;
                        }
                        break;
                    }
                }
                self.expect(&Tok::RBrace)?;
                Ok(Expr::Dict(kvs))
            }
            _ => Err(self.err_at(tok.line, tok.col, "Unexpected token in expression")),
        }
    }

    fn peek_binop(&self) -> Option<(BinOp, u8)> {
        if let Some(Token {
            kind: Tok::Op(op), ..
        }) = self.t.get(self.i)
        {
            let (b, p) = match op.as_str() {
                "==" => (BinOp::Eq, 2),
                "!=" => (BinOp::Ne, 2),
                "<" => (BinOp::Lt, 2),
                "<=" => (BinOp::Le, 2),
                ">" => (BinOp::Gt, 2),
                ">=" => (BinOp::Ge, 2),
                "+" => (BinOp::Add, 3),
                "-" => (BinOp::Sub, 3),
                "*" => (BinOp::Mul, 4),
                "/" => (BinOp::Div, 4),
                _ => return None,
            };
            Some((b, p))
        } else {
            None
        }
    }

    // ---------- helpers ----------

    fn eof(&self) -> bool {
        self.i >= self.t.len()
    }
    fn bump(&mut self) -> &Token {
        let t = &self.t[self.i];
        self.i += 1;
        t
    }
    fn peek(&self) -> Option<&Token> {
        self.t.get(self.i)
    }
    fn peek_is(&self, k: &Tok) -> bool {
        self.peek().map(|t| &t.kind == k).unwrap_or(false)
    }
    fn peek_n_is(&self, n: usize, k: &Tok) -> bool {
        self.t
            .get(self.i + n)
            .map(|t| &t.kind == k)
            .unwrap_or(false)
    }
    fn peek_op(&self, s: &str) -> bool {
        matches!(self.peek(), Some(Token { kind: Tok::Op(op), .. }) if op == s)
    }
    fn peek_kw(&self, s: &str) -> bool {
        matches!(self.peek(), Some(Token { kind: Tok::Keyword(k), .. }) if k == s)
    }
    fn expect_kw(&mut self, s: &str) -> RResult<()> {
        if self.peek_kw(s) {
            self.bump();
            Ok(())
        } else {
            Err(self.err_here(&format!("Expected keyword '{s}'")))
        }
    }
    fn expect(&mut self, k: &Tok) -> RResult<()> {
        if self.peek_is(k) {
            self.bump();
            Ok(())
        } else {
            Err(self.err_here(&format!("Expected {k:?}")))
        }
    }
    fn expect_newline(&mut self) -> RResult<()> {
        if self.peek_is(&Tok::Newline) {
            self.bump();
            Ok(())
        } else {
            Err(self.err_here("Expected newline"))
        }
    }
    fn expect_ident(&mut self) -> RResult<String> {
        match self.bump().kind.clone() {
            Tok::Ident(s) => Ok(s),
            Tok::Keyword(s) => Ok(s),
            _ => Err(self.err_here("Expected identifier")),
        }
    }
    fn peek_ident_string(&self) -> Option<String> {
        match self.peek() {
            Some(Token {
                kind: Tok::Ident(s),
                ..
            }) => Some(s.clone()),
            Some(Token {
                kind: Tok::Keyword(s),
                ..
            }) => Some(s.clone()),
            _ => None,
        }
    }
    fn skip_newlines(&mut self) {
        while self.peek_is(&Tok::Newline) {
            self.bump();
        }
    }
    fn err_here(&self, msg: &str) -> RelayError {
        let (line, col) = self.peek().map(|t| (t.line, t.col)).unwrap_or((0, 0));
        RelayError::Syntax {
            msg: msg.into(),
            line,
            col,
        }
    }
    fn err_at(&self, line: usize, col: usize, msg: &str) -> RelayError {
        RelayError::Syntax {
            msg: msg.into(),
            line,
            col,
        }
    }
}

// ========================= Values / Deferred =========================

type BoxFut = Pin<Box<dyn Future<Output = RResult<Value>> + Send>>;

#[derive(Clone)]
enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
    None,

    List(Vec<Value>),
    Dict(IndexMap<String, Value>), // keys coerced to string

    Json(J),
    Bytes(Vec<u8>),

    Function(Arc<UserFunction>),
    Builtin(Arc<BuiltinFn>),

    Deferred(Arc<Deferred>),
    Task(Arc<TaskHandle>),

    Thunk(Arc<Thunk>),

    Obj(Object),
    Response(Response),
}

#[derive(Clone)]
struct Response {
    status: u16,
    content_type: String,
    body: Vec<u8>,
    headers: HashMap<String, String>,
    set_cookies: Vec<String>,
}

#[derive(Clone)]
enum Object {
    WebApp(WebAppHandle),
    Route(RouteHandle),
    WebServer(WebServerHandle),
    Http(HttpHandle),
    HttpResponse(HttpResponseHandle),
    MongoClient(MongoClientHandle),
    MongoDatabase(MongoDatabaseHandle),
    MongoCollection(MongoCollectionHandle),
}

#[derive(Clone)]
struct UserFunction {
    name: String,
    params: Vec<Param>,
    body: Vec<Stmt>,
    decorators: Vec<Decorator>,
}

type BuiltinFn = dyn Fn(Vec<Value>, Vec<(String, Value)>) -> BoxFut + Send + Sync + 'static;

struct Deferred {
    fut: tokio::sync::Mutex<Option<BoxFut>>,
    cached: tokio::sync::Mutex<Option<Value>>,
}
impl Deferred {
    fn new(f: BoxFut) -> Self {
        Self {
            fut: tokio::sync::Mutex::new(Some(f)),
            cached: tokio::sync::Mutex::new(None),
        }
    }
    async fn resolve(&self) -> RResult<Value> {
        if let Some(v) = self.cached.lock().await.clone() {
            return Ok(v);
        }
        let mut g = self.fut.lock().await;
        let f = g
            .take()
            .ok_or_else(|| RelayError::Runtime("Deferred already taken".into()))?;
        drop(g);
        let v = f.await?;
        *self.cached.lock().await = Some(v.clone());
        Ok(v)
    }
}

struct TaskHandle {
    join: tokio::sync::Mutex<Option<tokio::task::JoinHandle<RResult<Value>>>>,
}
impl TaskHandle {
    fn new(j: tokio::task::JoinHandle<RResult<Value>>) -> Self {
        Self {
            join: tokio::sync::Mutex::new(Some(j)),
        }
    }
    async fn join(&self) -> RResult<Value> {
        let mut g = self.join.lock().await;
        let h = g
            .take()
            .ok_or_else(|| RelayError::Runtime("Task already joined".into()))?;
        drop(g);
        h.await
            .map_err(|e| RelayError::Runtime(format!("Task join error: {e}")))?
    }
}

// A Thunk is an unevaluated expression captured for delayed execution (used by schedulers like sleep/timeout).
struct Thunk {
    expr: Expr,
    env: Arc<tokio::sync::Mutex<Env>>,
}
impl Thunk {
    fn new(expr: Expr, env: Arc<tokio::sync::Mutex<Env>>) -> Self {
        Self { expr, env }
    }
    async fn run(&self) -> RResult<Value> {
        let ev = Evaluator::new(self.env.clone());
        ev.eval_expr(&self.expr).await
    }
}

impl Value {
    fn is_none(&self) -> bool {
        matches!(self, Value::None)
    }
    fn truthy(&self) -> bool {
        match self {
            Value::Bool(b) => *b,
            Value::Int(n) => *n != 0,
            Value::Float(f) => *f != 0.0,
            Value::Str(s) => !s.is_empty(),
            Value::List(v) => !v.is_empty(),
            Value::Dict(m) => !m.is_empty(),
            Value::None => false,
            _ => true,
        }
    }
    fn repr(&self) -> String {
        match self {
            Value::Int(n) => n.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => {
                if *b {
                    "True".into()
                } else {
                    "False".into()
                }
            }
            Value::Str(s) => s.clone(),
            Value::None => "None".into(),
            Value::List(v) => format!(
                "[{}]",
                v.iter().map(|x| x.repr()).collect::<Vec<_>>().join(", ")
            ),
            Value::Dict(m) => {
                let pairs = m
                    .iter()
                    .map(|(k, v)| format!("{k}: {}", v.repr()))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{pairs}}}")
            }
            Value::Json(j) => j.to_string(),
            Value::Bytes(b) => format!("<bytes {}>", b.len()),
            Value::Deferred(_) => "<Deferred>".into(),
            Value::Task(_) => "<Task>".into(),
            Value::Thunk(_) => "<Thunk>".into(),
            Value::Function(f) => format!("<fn {}>", f.name),
            Value::Builtin(_) => "<builtin>".into(),
            Value::Obj(_) => "<obj>".into(),
            Value::Response(_) => "<Response>".into(),
        }
    }
}

// ========================= Env =========================

#[derive(Clone)]
struct Env {
    scopes: Vec<HashMap<String, Value>>,
}
impl Env {
    fn new_global() -> Self {
        Self {
            scopes: vec![HashMap::new()],
        }
    }
    fn push(&mut self) {
        self.scopes.push(HashMap::new());
    }
    fn pop(&mut self) {
        self.scopes.pop();
    }
    fn set(&mut self, k: &str, v: Value) {
        self.scopes.last_mut().unwrap().insert(k.to_string(), v);
    }
    fn assign_existing(&mut self, k: &str, v: Value) -> bool {
        for scope in self.scopes.iter_mut().rev() {
            if scope.contains_key(k) {
                scope.insert(k.to_string(), v);
                return true;
            }
        }
        false
    }
    fn get(&self, k: &str) -> RResult<Value> {
        for scope in self.scopes.iter().rev() {
            if let Some(v) = scope.get(k) {
                return Ok(v.clone());
            }
        }
        Err(RelayError::Name(format!("Unknown name: {k}")))
    }
    fn snapshot_top(&self) -> HashMap<String, Value> {
        self.scopes.last().unwrap().clone()
    }

    fn snapshot_merged(&self) -> HashMap<String, Value> {
        let mut merged = HashMap::new();
        for scope in &self.scopes {
            for (k, v) in scope {
                merged.insert(k.clone(), v.clone());
            }
        }
        merged
    }
}

// ========================= Template strings =========================

fn render_template(s: &str, locals: &HashMap<String, Value>) -> String {
    // Replace {{ key }} with HTML-escaped value.repr()
    let mut out = String::new();
    let mut i = 0;
    while let Some(start) = s[i..].find("{{") {
        let start = i + start;
        out.push_str(&s[i..start]);
        if let Some(end) = s[start + 2..].find("}}") {
            let end = start + 2 + end;
            let key = s[start + 2..end].trim();
            if let Some(v) = locals.get(key) {
                out.push_str(&encode_safe(&v.repr()));
            }
            i = end + 2;
        } else {
            out.push_str(&s[start..]);
            return out;
        }
    }
    out.push_str(&s[i..]);
    out
}

// ========================= Evaluator (async, implicit Deferred) =========================

struct Evaluator {
    env: Arc<tokio::sync::Mutex<Env>>,
    module_state: Arc<tokio::sync::Mutex<ModuleState>>,
}

#[derive(Debug, Clone)]
struct ModuleState {
    loaded: HashSet<PathBuf>,
    stack: Vec<PathBuf>,
}

enum Flow {
    None,
    Return(Value),
}

impl Evaluator {
    fn new(env: Arc<tokio::sync::Mutex<Env>>) -> Self {
        Self {
            env,
            module_state: Arc::new(tokio::sync::Mutex::new(ModuleState {
                loaded: HashSet::new(),
                stack: Vec::new(),
            })),
        }
    }

    async fn snapshot_env(&self) -> Arc<tokio::sync::Mutex<Env>> {
        let env = self.env.lock().await;
        Arc::new(tokio::sync::Mutex::new(env.clone()))
    }

    async fn eval_program(&self, p: &Program) -> RResult<Value> {
        let mut last = Value::None;
        for s in &p.stmts {
            match self.eval_stmt(s).await? {
                Flow::None => {}
                Flow::Return(v) => return Ok(v),
            }
            last = Value::None;
        }
        Ok(last)
    }

    async fn eval_program_in_file(&self, p: &Program, file_path: PathBuf) -> RResult<Value> {
        {
            let mut state = self.module_state.lock().await;
            state.loaded.insert(file_path.clone());
            state.stack.push(file_path);
        }
        let result = self.eval_program(p).await;
        {
            let mut state = self.module_state.lock().await;
            state.stack.pop();
        }
        result
    }

    fn module_name_to_relative_path(module: &str) -> PathBuf {
        if module.ends_with(".ry") || module.contains('/') {
            return PathBuf::from(module);
        }
        let mut pb = PathBuf::new();
        for part in module.split('.') {
            pb.push(part);
        }
        pb.set_extension("ry");
        pb
    }

    async fn import_module(&self, module: &str) -> RResult<()> {
        let module_path = Self::module_name_to_relative_path(module);
        let base_dir = {
            let state = self.module_state.lock().await;
            state
                .stack
                .last()
                .and_then(|p| p.parent().map(|parent| parent.to_path_buf()))
                .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
        };
        let candidate = base_dir.join(module_path);
        let canonical = tokio::fs::canonicalize(&candidate).await.map_err(|e| {
            RelayError::Runtime(format!(
                "Failed to resolve module '{module}' from '{}': {e}",
                candidate.display()
            ))
        })?;

        {
            let state = self.module_state.lock().await;
            if state.loaded.contains(&canonical) {
                return Ok(());
            }
        }

        let src = tokio::fs::read_to_string(&canonical).await.map_err(|e| {
            RelayError::Runtime(format!(
                "Failed to read module '{}': {e}",
                canonical.display()
            ))
        })?;
        let mut lexer = Lexer::new(&src);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser::new(tokens);
        let program = parser.parse_program()?;
        self.eval_program_in_file(&program, canonical).await?;
        Ok(())
    }

    async fn eval_block(&self, b: &[Stmt]) -> RResult<Flow> {
        for s in b {
            match self.eval_stmt(s).await? {
                Flow::None => {}
                r @ Flow::Return(_) => return Ok(r),
            }
        }
        Ok(Flow::None)
    }

    async fn eval_block_implicit_return(&self, b: &[Stmt]) -> RResult<Value> {
        if b.is_empty() {
            return Ok(Value::None);
        }
        for (idx, s) in b.iter().enumerate() {
            let is_last = idx + 1 == b.len();
            if !is_last {
                match self.eval_stmt(s).await? {
                    Flow::None => {}
                    Flow::Return(v) => return Ok(v),
                }
                continue;
            }

            match s {
                Stmt::Expr(e) => return self.eval_expr(e).await,
                _ => match self.eval_stmt(s).await? {
                    Flow::None => return Ok(Value::None),
                    Flow::Return(v) => return Ok(v),
                },
            }
        }
        Ok(Value::None)
    }

    #[async_recursion]
    async fn eval_stmt(&self, s: &Stmt) -> RResult<Flow> {
        match s {
            Stmt::Expr(e) => {
                let v = self.eval_expr(e).await?;

                // Non-blocking: expression statements never wait.
                // If the result is Deferred/Task, run it in the background and keep the process alive.
                match v {
                    Value::Deferred(d) => {
                        let h = tokio::spawn(async move {
                            let _ = d.resolve().await;
                        });
                        track_bg(h).await;
                    }
                    Value::Task(t) => {
                        let h = tokio::spawn(async move {
                            let _ = t.join().await;
                        });
                        track_bg(h).await;
                    }
                    _ => {}
                }

                Ok(Flow::None)
            }
            Stmt::Import { module } => {
                self.import_module(module).await?;
                Ok(Flow::None)
            }
            Stmt::Assign { name, expr } => {
                let v = self.eval_expr(expr).await?;
                let mut env = self.env.lock().await;
                if !env.assign_existing(name, v.clone()) {
                    env.set(name, v);
                }
                Ok(Flow::None)
            }
            Stmt::AugAssign { name, op, expr } => {
                let rhs = self
                    .resolve_deferred_only(self.eval_expr(expr).await?)
                    .await?;
                let env = self.env.lock().await;
                let cur = env.get(name)?;
                drop(env);
                let cur = self.resolve_if_needed(cur).await?;
                let out = match op {
                    AugOp::Add => bin_add(cur, rhs)?,
                    AugOp::Sub => bin_sub(cur, rhs)?,
                };
                let mut env = self.env.lock().await;
                if !env.assign_existing(name, out.clone()) {
                    env.set(name, out);
                }
                Ok(Flow::None)
            }
            Stmt::DestructureAssign { targets, expr } => {
                let itv = self.resolve_if_needed(self.eval_expr(expr).await?).await?;
                let items = self.to_iter_items(itv, "Destructuring assignment")?;
                if items.len() != targets.len() {
                    return Err(RelayError::Runtime(format!(
                        "Destructuring assignment expected {} values, got {}",
                        targets.len(),
                        items.len()
                    )));
                }

                let mut env = self.env.lock().await;
                for (name, value) in targets.iter().zip(items.into_iter()) {
                    if !env.assign_existing(name, value.clone()) {
                        env.set(name, value);
                    }
                }
                Ok(Flow::None)
            }
            Stmt::Return(e) => {
                let v = if let Some(e) = e {
                    self.resolve_deferred_only(self.eval_expr(e).await?).await?
                } else {
                    Value::None
                };
                Ok(Flow::Return(v))
            }
            Stmt::If {
                cond,
                then_block,
                else_block,
            } => {
                let c = self.resolve_if_needed(self.eval_expr(cond).await?).await?;
                let mut env = self.env.lock().await;
                env.push();
                drop(env);
                let r = if c.truthy() {
                    self.eval_block(then_block).await?
                } else {
                    self.eval_block(else_block).await?
                };
                let mut env = self.env.lock().await;
                env.pop();
                Ok(r)
            }
            Stmt::While { cond, body } => {
                loop {
                    let c = self.resolve_if_needed(self.eval_expr(cond).await?).await?;
                    if !c.truthy() {
                        break;
                    }
                    let mut env = self.env.lock().await;
                    env.push();
                    drop(env);
                    let r = self.eval_block(body).await?;
                    let mut env = self.env.lock().await;
                    env.pop();
                    drop(env);
                    if let Flow::Return(v) = r {
                        return Ok(Flow::Return(v));
                    }
                }
                Ok(Flow::None)
            }
            Stmt::For { var, iter, body } => {
                let itv = self.resolve_if_needed(self.eval_expr(iter).await?).await?;
                let items = self.to_iter_items(itv, "for(each in iterable)")?;

                for item in items {
                    let mut env = self.env.lock().await;
                    env.push();
                    env.set(var, item);
                    drop(env);
                    let r = self.eval_block(body).await?;
                    let mut env = self.env.lock().await;
                    env.pop();
                    drop(env);
                    if let Flow::Return(v) = r {
                        return Ok(Flow::Return(v));
                    }
                }
                Ok(Flow::None)
            }
            Stmt::TryExcept {
                err_name,
                try_block,
                except_block,
            } => match self.eval_block(try_block).await {
                Ok(flow) => Ok(flow),
                Err(e) => {
                    let mut env = self.env.lock().await;
                    env.push();
                    if let Some(name) = err_name {
                        env.set(name, Value::Str(e.to_string()));
                    }
                    drop(env);

                    let result = self.eval_block(except_block).await;

                    let mut env = self.env.lock().await;
                    env.pop();
                    drop(env);

                    result
                }
            },
            Stmt::FuncDef {
                name,
                params,
                body,
                decorators,
            } => {
                let f = UserFunction {
                    name: name.clone(),
                    params: params.clone(),
                    body: body.clone(),
                    decorators: decorators.clone(),
                };

                // define function
                {
                    let mut env = self.env.lock().await;
                    env.set(name, Value::Function(Arc::new(f.clone())));
                }

                // register decorators immediately (requires the base route variable already exists)
                self.register_decorators_for_fn(name, &f.decorators).await?;

                Ok(Flow::None)
            }
        }
    }

    #[async_recursion]
    async fn eval_expr(&self, e: &Expr) -> RResult<Value> {
        match e {
            Expr::Int(n) => Ok(Value::Int(*n)),
            Expr::Float(f) => Ok(Value::Float(*f)),
            Expr::Bool(b) => Ok(Value::Bool(*b)),
            Expr::None => Ok(Value::None),
            Expr::Str(s) => {
                if s.contains("{{") && s.contains("}}") {
                    let locals = {
                        let env = self.env.lock().await;
                        env.snapshot_merged()
                    };
                    Ok(Value::Str(render_template(s, &locals)))
                } else {
                    Ok(Value::Str(s.clone()))
                }
            }
            Expr::Ident(s) => self.env.lock().await.get(s),

            Expr::List(items) => {
                let mut v = Vec::new();
                for it in items {
                    v.push(self.resolve_if_needed(self.eval_expr(it).await?).await?);
                }
                Ok(Value::List(v))
            }
            Expr::ListComp {
                expr,
                var,
                iter,
                filter,
            } => {
                let itv = self.resolve_if_needed(self.eval_expr(iter).await?).await?;
                let items = self.to_iter_items(itv, "list comprehension")?;
                let mut out = Vec::new();

                for item in items {
                    let mut env = self.env.lock().await;
                    env.push();
                    env.set(var, item);
                    drop(env);

                    let should_include = if let Some(cond) = filter {
                        let cv = self.resolve_if_needed(self.eval_expr(cond).await?).await?;
                        cv.truthy()
                    } else {
                        true
                    };

                    if should_include {
                        let v = self.resolve_if_needed(self.eval_expr(expr).await?).await?;
                        out.push(v);
                    }

                    let mut env = self.env.lock().await;
                    env.pop();
                }

                Ok(Value::List(out))
            }
            Expr::Dict(kvs) => {
                let mut m = IndexMap::new();
                for (k, v) in kvs {
                    let kk = self.resolve_if_needed(self.eval_expr(k).await?).await?;
                    let vv = self.resolve_if_needed(self.eval_expr(v).await?).await?;
                    m.insert(kk.repr(), vv);
                }
                Ok(Value::Dict(m))
            }

            Expr::Member { object, name } => {
                // IMPORTANT: member access must NOT auto-resolve Task/Deferred for .join/.resolve,
                // otherwise t.join() turns into (t auto-joined -> None).join -> type error.
                let raw = self.eval_expr(object).await?;

                let obj = match (&raw, name.as_str()) {
                    (Value::Task(_), "join") => raw,
                    (Value::Deferred(_), "join") => raw,
                    (Value::Deferred(_), "resolve") => raw,
                    _ => self.resolve_if_needed(raw).await?,
                };

                self.get_member(obj, name).await
            }

            Expr::Index { target, index } => {
                let t = self
                    .resolve_if_needed(self.eval_expr(target).await?)
                    .await?;
                let idx = self.resolve_if_needed(self.eval_expr(index).await?).await?;
                match (t, idx) {
                    (Value::List(v), Value::Int(i)) => v
                        .get(i as usize)
                        .cloned()
                        .ok_or_else(|| RelayError::Runtime("Index out of range".into())),
                    (Value::Str(s), Value::Int(i)) => s
                        .chars()
                        .nth(i as usize)
                        .map(|c| Value::Str(c.to_string()))
                        .ok_or_else(|| RelayError::Runtime("Index out of range".into())),
                    (Value::Dict(m), k) => m
                        .get(&k.repr())
                        .cloned()
                        .ok_or_else(|| RelayError::Runtime("Key not found".into())),
                    (Value::Json(j), Value::Str(k)) => Ok(match j.get(&k) {
                        Some(v) => Value::Json(v.clone()),
                        None => return Err(RelayError::Runtime("Key not found".into())),
                    }),
                    _ => Err(RelayError::Type(
                        "Indexing expects list/string/dict/json".into(),
                    )),
                }
            }

            Expr::Call {
                callee,
                args,
                kwargs,
            } => {
                // callee can be Member(...) which may return a bound method
                // IMPORTANT: spawn(expr) must receive raw (unresolved) values so it can run them concurrently
                let is_spawn_ident = matches!(&**callee, Expr::Ident(name) if name == "spawn");
                let is_sleep_ident = matches!(&**callee, Expr::Ident(name) if name == "sleep");
                let is_print_ident = matches!(&**callee, Expr::Ident(name) if name == "print");

                let c = self
                    .resolve_deferred_only(self.eval_expr(callee).await?)
                    .await?;

                // args
                let mut a = Vec::new();
                for (idx, x) in args.iter().enumerate() {
                    if (is_sleep_ident && idx == 1) || is_print_ident {
                        // Pass unevaluated expression as a thunk (evaluated after delay or in print)
                        let env = self.snapshot_env().await;
                        a.push(Value::Thunk(Arc::new(Thunk::new(x.clone(), env))));
                        continue;
                    }
                    let v = self.eval_expr(x).await?;
                    let v = if is_spawn_ident {
                        v
                    } else {
                        self.resolve_deferred_only(v).await?
                    };
                    a.push(v);
                }

                // kwargs
                let mut kw = Vec::new();
                for (k, x) in kwargs {
                    let v = self.eval_expr(x).await?;
                    let v = if is_spawn_ident {
                        v
                    } else {
                        self.resolve_deferred_only(v).await?
                    };
                    kw.push((k.clone(), v));
                }

                self.call_value(c, a, kw).await
            }

            Expr::BinOp { left, op, right } => {
                let l = self.resolve_if_needed(self.eval_expr(left).await?).await?;
                let r = self.resolve_if_needed(self.eval_expr(right).await?).await?;
                eval_bin(*op, l, r)
            }

            Expr::Unary { op, expr } => {
                let v = self.resolve_if_needed(self.eval_expr(expr).await?).await?;
                match op {
                    UnaryOp::Neg => match v {
                        Value::Int(n) => Ok(Value::Int(-n)),
                        Value::Float(f) => Ok(Value::Float(-f)),
                        _ => Err(RelayError::Type("Unary '-' expects number".into())),
                    },
                    UnaryOp::Not => Ok(Value::Bool(!v.truthy())),
                }
            }
        }
    }

    async fn resolve_if_needed(&self, v: Value) -> RResult<Value> {
        match v {
            Value::Deferred(d) => d.resolve().await,
            Value::Task(t) => t.join().await,
            other => Ok(other),
        }
    }

    async fn resolve_deferred_only(&self, v: Value) -> RResult<Value> {
        match v {
            Value::Deferred(d) => d.resolve().await,
            other => Ok(other),
        }
    }

    fn to_iter_items(&self, value: Value, context: &str) -> RResult<Vec<Value>> {
        match value {
            Value::List(v) => Ok(v),
            Value::Str(s) => Ok(s.chars().map(|c| Value::Str(c.to_string())).collect()),
            Value::Dict(m) => Ok(m.keys().map(|k| Value::Str(k.clone())).collect()),
            _ => Err(RelayError::Type(format!(
                "{context} expects list/string/dict"
            ))),
        }
    }

    async fn call_value(
        &self,
        callee: Value,
        args: Vec<Value>,
        kwargs: Vec<(String, Value)>,
    ) -> RResult<Value> {
        match callee {
            Value::Builtin(f) => (f)(args, kwargs).await,
            Value::Function(f) => self.call_user_fn(&f, args, kwargs).await,
            Value::Obj(o) => o.call(self, args, kwargs).await,
            Value::Response(r) => Ok(Value::Response(r)),
            _ => Err(RelayError::Type("Value is not callable".into())),
        }
    }

    async fn call_user_fn(
        &self,
        f: &UserFunction,
        args: Vec<Value>,
        kwargs: Vec<(String, Value)>,
    ) -> RResult<Value> {
        // bind positional + kwargs + defaults
        let mut bound: HashMap<String, Value> = HashMap::new();

        for (i, p) in f.params.iter().enumerate() {
            if i < args.len() {
                bound.insert(p.name.clone(), args[i].clone());
            }
        }
        for (k, v) in kwargs {
            bound.insert(k, v);
        }
        for p in &f.params {
            if !bound.contains_key(&p.name) {
                if let Some(def) = &p.default {
                    bound.insert(
                        p.name.clone(),
                        self.resolve_if_needed(self.eval_expr(def).await?).await?,
                    );
                } else {
                    bound.insert(p.name.clone(), Value::None);
                }
            }
        }

        // type hints: enforce minimal (str/int/float/Json)
        for p in &f.params {
            if let Some(ty) = &p.ty {
                if let Some(v) = bound.get(&p.name).cloned() {
                    bound.insert(p.name.clone(), coerce_param_type(ty, v)?);
                }
            }
        }

        // execute
        {
            let mut env = self.env.lock().await;
            env.push();
            for (k, v) in &bound {
                env.set(k, v.clone());
            }
        }

        let result = self.eval_block_implicit_return(&f.body).await?;

        {
            let mut env = self.env.lock().await;
            env.pop();
        }

        Ok(result)
    }

    async fn get_member(&self, obj: Value, name: &str) -> RResult<Value> {
        match obj {
            Value::Obj(o) => o.get_member(name),

            Value::Task(t) => {
                match name {
                    "join" => {
                        // return a bound method: t.join()
                        let t = t.clone();
                        Ok(Value::Builtin(Arc::new(move |_args, _kwargs| {
                            let t = t.clone();
                            Box::pin(async move {
                                // join is async, return Deferred so it fits Relay's model
                                let d = Deferred::new(Box::pin(async move {
                                    let v = t.join().await?;
                                    Ok(v)
                                }));
                                Ok(Value::Deferred(Arc::new(d)))
                            })
                        })))
                    }
                    _ => Ok(Value::None),
                }
            }

            Value::Deferred(d) => match name {
                "join" | "resolve" => {
                    let d0 = d.clone();
                    Ok(Value::Builtin(Arc::new(move |_args, _kwargs| {
                        let d0 = d0.clone();
                        Box::pin(async move {
                            let d2 = Deferred::new(Box::pin(async move {
                                let v = d0.resolve().await?;
                                Ok(v)
                            }));
                            Ok(Value::Deferred(Arc::new(d2)))
                        })
                    })))
                }
                _ => Ok(Value::None),
            },

            Value::Dict(m) => Ok(m.get(name).cloned().unwrap_or(Value::None)),
            Value::Json(j) => Ok(j.get(name).cloned().map(Value::Json).unwrap_or(Value::None)),

            _ => Err(RelayError::Type(
                "Member access expects object/dict/json/task/deferred".into(),
            )),
        }
    }

    async fn register_decorators_for_fn(
        &self,
        fn_name: &str,
        decorators: &[Decorator],
    ) -> RResult<()> {
        for d in decorators {
            match d {
                Decorator::Route { base, method, path } => {
                    let base_val = self.env.lock().await.get(base)?;
                    let base_val = self.resolve_if_needed(base_val).await?;
                    match base_val {
                        // New spec: @app.get("/path") where `app` is a WebApp
                        Value::Obj(Object::WebApp(a)) => {
                            a.register(method.clone(), path.clone(), fn_name.to_string());
                        }
                        // Back-compat: route = app.route(); @route.get("/path")
                        Value::Obj(Object::Route(r)) => {
                            r.register(method.clone(), path.clone(), fn_name.to_string());
                        }
                        _ => {
                            return Err(RelayError::Type(format!(
                            "@{base}.* expects `{base}` to be a WebApp (preferred) or Route handle"
                        )))
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

fn coerce_param_type(ty: &str, v: Value) -> RResult<Value> {
    match ty {
        "str" => Ok(Value::Str(v.repr())),
        "int" => match v {
            Value::Int(n) => Ok(Value::Int(n)),
            Value::Float(f) => Ok(Value::Int(f as i64)),
            Value::Str(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::Int)
                .map_err(|_| RelayError::Type("Bad int".into())),
            _ => Err(RelayError::Type("Bad int".into())),
        },
        "float" => match v {
            Value::Float(f) => Ok(Value::Float(f)),
            Value::Int(n) => Ok(Value::Float(n as f64)),
            Value::Str(s) => s
                .trim()
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| RelayError::Type("Bad float".into())),
            _ => Err(RelayError::Type("Bad float".into())),
        },
        "Json" | "json" => match v {
            Value::Json(j) => Ok(Value::Json(j)),
            Value::Dict(m) => {
                let mut obj = serde_json::Map::new();
                for (k, vv) in m {
                    obj.insert(k, J::String(vv.repr()));
                }
                Ok(Value::Json(J::Object(obj)))
            }
            Value::Str(s) => serde_json::from_str::<J>(&s)
                .map(Value::Json)
                .map_err(|_| RelayError::Type("Bad Json".into())),
            _ => Err(RelayError::Type("Bad Json".into())),
        },
        _ => Ok(v),
    }
}

// ========================= Binary ops =========================

fn eval_bin(op: BinOp, l: Value, r: Value) -> RResult<Value> {
    use BinOp::*;
    Ok(match op {
        Add => bin_add(l, r)?,
        Sub => bin_sub(l, r)?,
        Mul => bin_mul(l, r)?,
        Div => bin_div(l, r)?,
        Eq => Value::Bool(l.repr() == r.repr()),
        Ne => Value::Bool(l.repr() != r.repr()),
        Lt => Value::Bool(cmp_num(&l)? < cmp_num(&r)?),
        Le => Value::Bool(cmp_num(&l)? <= cmp_num(&r)?),
        Gt => Value::Bool(cmp_num(&l)? > cmp_num(&r)?),
        Ge => Value::Bool(cmp_num(&l)? >= cmp_num(&r)?),
    })
}
fn cmp_num(v: &Value) -> RResult<f64> {
    match v {
        Value::Int(n) => Ok(*n as f64),
        Value::Float(f) => Ok(*f),
        _ => Err(RelayError::Type("Comparison expects numbers".into())),
    }
}
fn bin_add(l: Value, r: Value) -> RResult<Value> {
    match (l, r) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
        (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 + b)),
        (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + b as f64)),
        (Value::Str(a), Value::Str(b)) => Ok(Value::Str(a + &b)),
        (a, b) => Ok(Value::Str(a.repr() + &b.repr())),
    }
}
fn bin_sub(l: Value, r: Value) -> RResult<Value> {
    match (l, r) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
        (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 - b)),
        (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - b as f64)),
        _ => Err(RelayError::Type("'-' expects numbers".into())),
    }
}
fn bin_mul(l: Value, r: Value) -> RResult<Value> {
    match (l, r) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
        (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 * b)),
        (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * b as f64)),
        _ => Err(RelayError::Type("'*' expects numbers".into())),
    }
}
fn bin_div(l: Value, r: Value) -> RResult<Value> {
    match (l, r) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Float(a as f64 / b as f64)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a / b)),
        (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 / b)),
        (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a / b as f64)),
        _ => Err(RelayError::Type("'/' expects numbers".into())),
    }
}

// ========================= Stdlib =========================

fn install_stdlib(env: &mut Env, evaluator: Arc<Evaluator>) -> RResult<()> {
    // print(...)
    env.set(
        "print",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let needs_async = args
                    .iter()
                    .any(|v| matches!(v, Value::Thunk(_) | Value::Deferred(_) | Value::Task(_)));
                if needs_async {
                    let d = Deferred::new(Box::pin(async move {
                        let mut out = Vec::new();
                        for v in args {
                            let mut resolved = match v {
                                Value::Thunk(t) => t.run().await?,
                                Value::Deferred(d) => d.resolve().await?,
                                Value::Task(t) => t.join().await?,
                                v => v,
                            };
                            resolved = match resolved {
                                Value::Deferred(d) => d.resolve().await?,
                                Value::Task(t) => t.join().await?,
                                v => v,
                            };
                            out.push(resolved);
                        }
                        println!(
                            "{}",
                            out.iter().map(|v| v.repr()).collect::<Vec<_>>().join(" ")
                        );
                        Ok(Value::None)
                    }));
                    Ok(Value::Deferred(Arc::new(d)))
                } else {
                    println!(
                        "{}",
                        args.iter().map(|v| v.repr()).collect::<Vec<_>>().join(" ")
                    );
                    Ok(Value::None)
                }
            })
        })),
    );

    // converters: str/int/float
    env.set(
        "str",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                Ok(Value::Str(
                    args.get(0).cloned().unwrap_or(Value::None).repr(),
                ))
            })
        })),
    );
    env.set(
        "int",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let v = args.get(0).cloned().unwrap_or(Value::None);
                coerce_param_type("int", v)
            })
        })),
    );
    env.set(
        "float",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let v = args.get(0).cloned().unwrap_or(Value::None);
                coerce_param_type("float", v)
            })
        })),
    );

    // sleep(ms, value=None) -> Deferred
    // Non-blocking scheduler primitive:
    // - returns immediately with a Deferred
    // - after ms, evaluates/resolves the 2nd argument (Thunk/Deferred/Task/callable/value)
    // - prints non-None result and resolves to it
    env.set(
        "sleep",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let ms = expect_int(&args, 0, "sleep(ms, value?)")? as u64;
                let value = args.get(1).cloned().unwrap_or(Value::None);

                let d = Deferred::new(Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(ms)).await;

                    // Evaluate/resolve after delay
                    let mut out = match value {
                        Value::Thunk(t) => t.run().await?,
                        Value::Deferred(d) => d.resolve().await?,
                        Value::Task(t) => t.join().await?,
                        Value::Builtin(f) => (f)(vec![], vec![]).await?, // sleep(ms, fn)
                        Value::Function(f) => {
                            // call user fn with no args after delay
                            // we cannot access evaluator here, so return the function itself;
                            // if you want sleep(ms, user_fn) to call it, wrap as sleep(ms, spawn(user_fn()))
                            Value::Function(f)
                        }
                        v => v,
                    };

                    // If evaluation yielded Deferred/Task, resolve now
                    out = match out {
                        Value::Deferred(d) => d.resolve().await?,
                        Value::Task(t) => t.join().await?,
                        v => v,
                    };

                    if !out.is_none() {
                        println!("{}", out.repr());
                    }
                    Ok(out)
                }));
                Ok(Value::Deferred(Arc::new(d)))
            })
        })),
    );
    // timeout(expr, ms)
    env.set(
        "timeout",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                if args.len() < 2 {
                    return Err(RelayError::Type("timeout(expr, ms)".into()));
                }
                let expr = args[0].clone();
                let ms = expect_int(&args, 1, "timeout(expr, ms)")? as u64;
                let d = Deferred::new(Box::pin(async move {
                    let fut = async move {
                        match expr {
                            Value::Deferred(d) => d.resolve().await,
                            Value::Task(t) => t.join().await,
                            v => Ok(v),
                        }
                    };
                    tokio::select! {
                        r = fut => r,
                        _ = tokio::time::sleep(Duration::from_millis(ms)) => Err(RelayError::Runtime("timeout".into())),
                    }
                }));
                Ok(Value::Deferred(Arc::new(d)))
            })
        })),
    );

    // fs: read_file / save_file
    env.set(
        "read_file",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let path = expect_str(&args, 0, "read_file(path)")?;
                let d = Deferred::new(Box::pin(async move {
                    let s = tokio::fs::read_to_string(path)
                        .await
                        .map_err(|e| RelayError::Runtime(e.to_string()))?;
                    Ok(Value::Str(s))
                }));
                Ok(Value::Deferred(Arc::new(d)))
            })
        })),
    );
    env.set(
        "save_file",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let text = expect_str(&args, 0, "save_file(text, path)")?;
                let path = expect_str(&args, 1, "save_file(text, path)")?;
                let d = Deferred::new(Box::pin(async move {
                    tokio::fs::write(path, text)
                        .await
                        .map_err(|e| RelayError::Runtime(e.to_string()))?;
                    Ok(Value::None)
                }));
                Ok(Value::Deferred(Arc::new(d)))
            })
        })),
    );

    // json: read_json / save_json
    env.set(
        "read_json",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let path = expect_str(&args, 0, "read_json(path)")?;
                let d = Deferred::new(Box::pin(async move {
                    let s = tokio::fs::read_to_string(path)
                        .await
                        .map_err(|e| RelayError::Runtime(e.to_string()))?;
                    let j: J =
                        serde_json::from_str(&s).map_err(|e| RelayError::Runtime(e.to_string()))?;
                    Ok(Value::Json(j))
                }));
                Ok(Value::Deferred(Arc::new(d)))
            })
        })),
    );
    env.set(
        "save_json",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let val = args
                    .get(0)
                    .cloned()
                    .ok_or_else(|| RelayError::Type("save_json(data, path) missing data".into()))?;
                let path = expect_str(&args, 1, "save_json(data, path)")?;
                let d = Deferred::new(Box::pin(async move {
                    let j = match val {
                        Value::Json(j) => j,
                        Value::Dict(m) => {
                            let mut obj = serde_json::Map::new();
                            for (k, vv) in m {
                                obj.insert(k, J::String(vv.repr()));
                            }
                            J::Object(obj)
                        }
                        Value::Str(s) => serde_json::from_str::<J>(&s).unwrap_or(J::String(s)),
                        other => J::String(other.repr()),
                    };
                    let s = serde_json::to_string_pretty(&j)
                        .map_err(|e| RelayError::Runtime(e.to_string()))?;
                    tokio::fs::write(path, s)
                        .await
                        .map_err(|e| RelayError::Runtime(e.to_string()))?;
                    Ok(Value::None)
                }));
                Ok(Value::Deferred(Arc::new(d)))
            })
        })),
    );

    // concurrency: spawn(expr), task.join(), cancel(task), all([..]), race([..])
    env.set(
        "spawn",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                if args.is_empty() {
                    return Err(RelayError::Type("spawn(expr_or_deferred)".into()));
                }
                let v = args[0].clone();
                let h = tokio::spawn(async move {
                    match v {
                        Value::Deferred(d) => d.resolve().await,
                        Value::Task(t) => t.join().await,
                        other => Ok(other),
                    }
                });
                Ok(Value::Task(Arc::new(TaskHandle::new(h))))
            })
        })),
    );

    env.set(
        "cancel",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let t = match args.get(0) {
                    Some(Value::Task(t)) => t.clone(),
                    _ => return Err(RelayError::Type("cancel(task) expects a Task".into())),
                };
                let mut g = t.join.lock().await;
                if let Some(h) = g.take() {
                    h.abort();
                }
                Ok(Value::None)
            })
        })),
    );

    env.set(
        "all",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let list = match args.get(0) {
                    Some(Value::List(v)) => v.clone(),
                    _ => return Err(RelayError::Type("all([..]) expects a list".into())),
                };
                let d = Deferred::new(Box::pin(async move {
                    let mut out = Vec::new();
                    for v in list {
                        let r = match v {
                            Value::Deferred(d) => d.resolve().await?,
                            Value::Task(t) => t.join().await?,
                            x => x,
                        };
                        out.push(r);
                    }
                    Ok(Value::List(out))
                }));
                Ok(Value::Deferred(Arc::new(d)))
            })
        })),
    );

    env.set(
        "race",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let list = match args.get(0) {
                    Some(Value::List(v)) => v.clone(),
                    _ => return Err(RelayError::Type("race([..]) expects a list".into())),
                };
                let d = Deferred::new(Box::pin(async move {
                    let mut join_set = tokio::task::JoinSet::new();
                    for v in list {
                        join_set.spawn(async move {
                            match v {
                                Value::Deferred(d) => d.resolve().await,
                                Value::Task(t) => t.join().await,
                                x => Ok(x),
                            }
                        });
                    }
                    match join_set.join_next().await {
                        Some(r) => r.map_err(|e| RelayError::Runtime(e.to_string()))?,
                        None => Err(RelayError::Runtime(
                            "race([..]) expects at least one task".into(),
                        )),
                    }
                }));
                Ok(Value::Deferred(Arc::new(d)))
            })
        })),
    );

    // Response(body, status=..., content_type=...)
    env.set(
        "Response",
        Value::Builtin(Arc::new(|args, kwargs| {
            Box::pin(async move {
                let status = kwargs
                    .iter()
                    .find(|(k, _)| k == "status")
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Value::Int(200));
                let ct = kwargs
                    .iter()
                    .find(|(k, _)| k == "content_type")
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Value::Str("text/plain; charset=utf-8".into()));
                let body = args.get(0).cloned().unwrap_or(Value::Str("".into()));
                let status = match status {
                    Value::Int(n) => n as u16,
                    _ => 200,
                };
                let ct = match ct {
                    Value::Str(s) => s,
                    _ => "text/plain; charset=utf-8".into(),
                };
                let bytes = match body {
                    Value::Str(s) => s.into_bytes(),
                    Value::Bytes(b) => b,
                    Value::Json(j) => j.to_string().into_bytes(),
                    v => v.repr().into_bytes(),
                };
                Ok(Value::Response(Response {
                    status,
                    content_type: ct,
                    body: bytes,
                    headers: HashMap::new(),
                    set_cookies: vec![],
                }))
            })
        })),
    );

    // Web framework
    env.set(
        "WebServer",
        Value::Builtin(Arc::new(|_args, _| {
            Box::pin(async move { Ok(Value::Obj(Object::WebServer(WebServerHandle::new()))) })
        })),
    );
    env.set(
        "WebApp",
        Value::Builtin(Arc::new(|_args, _| {
            Box::pin(async move { Ok(Value::Obj(Object::WebApp(WebAppHandle::new()))) })
        })),
    );

    // HTTP client
    env.set(
        "Http",
        Value::Builtin(Arc::new(|_args, _| {
            Box::pin(async move { Ok(Value::Obj(Object::Http(HttpHandle::new()))) })
        })),
    );

    // MongoDB client
    env.set(
        "Mongo",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let uri = expect_str(&args, 0, "Mongo(uri)")?;
                let d = Deferred::new(Box::pin(async move {
                    let client = MongoClient::with_uri_str(uri)
                        .await
                        .map_err(|e| RelayError::Runtime(e.to_string()))?;
                    Ok(Value::Obj(Object::MongoClient(MongoClientHandle::new(
                        client,
                    ))))
                }));
                Ok(Value::Deferred(Arc::new(d)))
            })
        })),
    );

    // Hook global callback for web requests -> interpreter calls
    // We call handler by name and inject params (Path > Body > Query)
    set_global_callback(Arc::new(move |app, fn_name, req| {
        let ev = evaluator.clone();
        Box::pin(async move { ev.call_web_handler(app, &fn_name, req).await })
    }));

    Ok(())
}

fn expect_str(args: &[Value], i: usize, sig: &str) -> RResult<String> {
    match args.get(i) {
        Some(Value::Str(s)) => Ok(s.clone()),
        Some(v) => Ok(v.repr()),
        None => Err(RelayError::Type(format!("{sig} missing arg"))),
    }
}
fn expect_int(args: &[Value], i: usize, sig: &str) -> RResult<i64> {
    match args.get(i) {
        Some(Value::Int(n)) => Ok(*n),
        Some(Value::Float(f)) => Ok(*f as i64),
        Some(Value::Str(s)) => s
            .trim()
            .parse()
            .map_err(|_| RelayError::Type(format!("{sig} expects int"))),
        _ => Err(RelayError::Type(format!("{sig} expects int"))),
    }
}

fn value_to_json(v: &Value) -> J {
    match v {
        Value::Json(j) => j.clone(),
        Value::Dict(m) => {
            let mut obj = serde_json::Map::new();
            for (k, vv) in m {
                obj.insert(k.clone(), value_to_json(vv));
            }
            J::Object(obj)
        }
        Value::List(vs) => J::Array(vs.iter().map(value_to_json).collect()),
        Value::Str(s) => J::String(s.clone()),
        Value::Int(n) => J::Number((*n).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(J::Number)
            .unwrap_or(J::Null),
        Value::Bool(b) => J::Bool(*b),
        Value::None => J::Null,
        Value::Bytes(b) => J::String(format!("<bytes {}>", b.len())),
        other => J::String(other.repr()),
    }
}

fn value_to_document(v: Value, sig: &str) -> RResult<Document> {
    let json = value_to_json(&v);
    match json {
        J::Object(_) => {
            bson::to_document(&json).map_err(|e| RelayError::Runtime(format!("{sig}: {e}")))
        }
        _ => Err(RelayError::Type(format!(
            "{sig} expects a dict/json object"
        ))),
    }
}

fn bson_to_json(bson: Bson) -> RResult<J> {
    serde_json::to_value(bson).map_err(|e| RelayError::Runtime(e.to_string()))
}

// ========================= Web runtime (Axum) =========================

#[derive(Clone)]
struct WebAppHandle {
    inner: Arc<Mutex<AppState>>,
}
#[derive(Clone)]
struct RouteHandle {
    app: WebAppHandle,
}
#[derive(Clone)]
struct WebServerHandle;

#[derive(Default)]
struct AppState {
    routes: Vec<RouteSpec>,
    middlewares: Vec<String>,
    static_mounts: Vec<StaticMount>,
    sessions: HashMap<String, IndexMap<String, Value>>,
}
#[derive(Clone)]
struct RouteSpec {
    method: String,
    path: String,
    fn_name: String,
}

#[derive(Clone)]
struct StaticMount {
    route_prefix: String,
    dir: String,
}

impl WebServerHandle {
    fn new() -> Self {
        Self
    }
}
impl WebAppHandle {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(AppState::default())),
        }
    }

    // Back-compat: route handle style (route = app.route(); @route.get("/"))
    fn route(&self) -> RouteHandle {
        RouteHandle { app: self.clone() }
    }

    // New spec: decorator attaches directly to the app: @app.get("/"), @app.post("/")
    fn register(&self, method: String, path: String, fn_name: String) {
        let mut st = self.inner.lock().unwrap();
        st.routes.push(RouteSpec {
            method,
            path,
            fn_name,
        });
    }

    fn use_middleware(&self, fn_name: String) {
        let mut st = self.inner.lock().unwrap();
        st.middlewares.push(fn_name);
    }

    fn mount_static(&self, route_prefix: String, dir: String) {
        let mut st = self.inner.lock().unwrap();
        st.static_mounts.push(StaticMount { route_prefix, dir });
    }

    fn middleware_names(&self) -> Vec<String> {
        let st = self.inner.lock().unwrap();
        st.middlewares.clone()
    }

    fn get_session(&self, sid: &str) -> IndexMap<String, Value> {
        let st = self.inner.lock().unwrap();
        st.sessions.get(sid).cloned().unwrap_or_default()
    }

    fn put_session(&self, sid: String, data: IndexMap<String, Value>) {
        let mut st = self.inner.lock().unwrap();
        st.sessions.insert(sid, data);
    }
}
impl RouteHandle {
    fn register(&self, method: String, path: String, fn_name: String) {
        let mut st = self.app.inner.lock().unwrap();
        st.routes.push(RouteSpec {
            method,
            path,
            fn_name,
        });
    }
}

// Callback from Axum -> interpreter
#[derive(Clone)]
struct RequestParts {
    method: String,
    path: String,
    path_params: HashMap<String, String>,
    query: HashMap<String, String>,
    json: Option<J>,
    headers: HashMap<String, String>,
    cookies: HashMap<String, String>,
    session_id: Option<String>,
}

type HandlerCb = Arc<dyn Fn(WebAppHandle, String, RequestParts) -> BoxFut + Send + Sync>;
static GLOBAL_CB: OnceLock<HandlerCb> = OnceLock::new();

static BG_TASKS: OnceLock<tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> = OnceLock::new();

fn bg_tasks() -> &'static tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>> {
    BG_TASKS.get_or_init(|| tokio::sync::Mutex::new(Vec::new()))
}

async fn track_bg(h: tokio::task::JoinHandle<()>) {
    let bg = bg_tasks();
    bg.lock().await.push(h);
}

async fn drain_bg() {
    let bg = bg_tasks();
    let mut g = bg.lock().await;
    let mut handles = Vec::new();
    std::mem::swap(&mut *g, &mut handles);
    drop(g);
    for h in handles {
        let _ = h.await;
    }
}

fn set_global_callback(cb: HandlerCb) {
    let _ = GLOBAL_CB.set(cb);
}

fn get_global_callback() -> Option<HandlerCb> {
    GLOBAL_CB.get().cloned()
}

async fn run_app(app: WebAppHandle) -> RResult<()> {
    let (specs, static_mounts) = {
        let state = app.inner.lock().unwrap();
        (state.routes.clone(), state.static_mounts.clone())
    };
    let mut router = Router::new();

    for mount in static_mounts {
        let route = normalize_static_route_prefix(&mount.route_prefix);
        let path_pattern = format!("{}/*file", route.trim_end_matches('/'));
        let dir = mount.dir.clone();

        let dir_for_index = dir.clone();
        let route_for_index = route.clone();
        let index_handler = move || {
            let dir = dir_for_index.clone();
            let route_prefix = route_for_index.clone();
            async move {
                let response = serve_static_file(&route_prefix, &dir, "index.html").await;
                Ok::<_, (StatusCode, String)>(value_to_axum_response(Value::Response(response)).await.into_response())
            }
        };

        let dir_for_files = dir.clone();
        let route_for_files = route.clone();
        let static_handler = move |Path(file): Path<String>| {
            let dir = dir_for_files.clone();
            let route_prefix = route_for_files.clone();
            async move {
                let response = serve_static_file(&route_prefix, &dir, &file).await;
                Ok::<_, (StatusCode, String)>(value_to_axum_response(Value::Response(response)).await.into_response())
            }
        };

        router = router
            .route(&route, get(index_handler))
            .route(&path_pattern, get(static_handler));
    }

    for r in specs {
        let method = r.method.to_lowercase();
        let path = relay_path_to_axum(&r.path);
        let fn_name = r.fn_name.clone();
        let route_path = r.path.clone();
        let app_handle = app.clone();

        // define handler in THIS scope so `get(handler)` / `post(handler)` can see it
        let handler = move |method: Method,
                            Path(ax_path): Path<HashMap<String, String>>,
                            Query(q): Query<HashMap<String, String>>,
                            headers: HeaderMap,
                            body: Option<AxumJson<J>>| {
            let fn_name = fn_name.clone();
            let route_path = route_path.clone();
            let app_handle = app_handle.clone();
            async move {
                let mut h = HashMap::new();
                for (k, v) in headers.iter() {
                    if let Ok(s) = v.to_str() {
                        h.insert(k.to_string(), s.to_string());
                    }
                }
                let cookies = parse_cookie_header(h.get("cookie").cloned());
                let session_id = cookies.get("relay_sid").cloned();

                let req = RequestParts {
                    method: method.to_string(),
                    path: route_path,
                    path_params: ax_path,
                    query: q,
                    json: body.map(|b| b.0),
                    headers: h,
                    cookies,
                    session_id,
                };

                let cb = get_global_callback().ok_or_else(|| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "No relay callback".to_string(),
                    )
                })?;

                let v = cb(app_handle, fn_name, req)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                Ok::<_, (StatusCode, String)>(value_to_axum_response(v).await.into_response())
            }
        };

        router = match method.as_str() {
            "get" => router.route(&path, get(handler)),
            "post" => router.route(&path, post(handler)),
            "put" => router.route(&path, axum::routing::put(handler)),
            "patch" => router.route(&path, axum::routing::patch(handler)),
            "delete" => router.route(&path, axum::routing::delete(handler)),
            _ => {
                return Err(RelayError::Runtime(format!(
                    "Unsupported HTTP method: {method}"
                )))
            }
        };
    }

    let addr = std::env::var("RELAY_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    println!("Relay WebServer listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| RelayError::Runtime(e.to_string()))?;
    axum::serve(listener, router)
        .await
        .map_err(|e| RelayError::Runtime(e.to_string()))?;
    Ok(())
}

fn parse_cookie_header(raw: Option<String>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(v) = raw {
        for seg in v.split(';') {
            let part = seg.trim();
            if part.is_empty() {
                continue;
            }
            if let Some((k, vv)) = part.split_once('=') {
                out.insert(k.trim().to_string(), vv.trim().to_string());
            }
        }
    }
    out
}

fn new_session_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("rsid_{now:x}")
}

fn relay_path_to_axum(p: &str) -> String {
    // "/api/<user_id>/x" -> "/api/:user_id/x"
    let mut out = String::new();
    let bytes = p.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] as char == '<' {
            if let Some(end) = p[i + 1..].find('>') {
                let name = &p[i + 1..i + 1 + end];
                out.push(':');
                out.push_str(name);
                i = i + 1 + end + 1;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn normalize_static_route_prefix(prefix: &str) -> String {
    if prefix.is_empty() || prefix == "/" {
        "/".to_string()
    } else {
        let mut normalized = prefix.to_string();
        if !normalized.starts_with('/') {
            normalized.insert(0, '/');
        }
        while normalized.len() > 1 && normalized.ends_with('/') {
            normalized.pop();
        }
        normalized
    }
}

fn safe_static_join(base: &str, rel_file: &str) -> Option<PathBuf> {
    let rel = PathBuf::from(rel_file);
    if rel.is_absolute() {
        return None;
    }
    if rel
        .components()
        .any(|c| matches!(c, Component::ParentDir | Component::Prefix(_)))
    {
        return None;
    }
    Some(PathBuf::from(base).join(rel))
}

fn guess_content_type(path: &str) -> &'static str {
    if let Some(ext) = path.rsplit('.').next() {
        match ext.to_ascii_lowercase().as_str() {
            "html" | "htm" => "text/html; charset=utf-8",
            "css" => "text/css; charset=utf-8",
            "js" | "mjs" => "application/javascript; charset=utf-8",
            "json" => "application/json",
            "svg" => "image/svg+xml",
            "png" => "image/png",
            "jpg" | "jpeg" => "image/jpeg",
            "gif" => "image/gif",
            "webp" => "image/webp",
            "txt" => "text/plain; charset=utf-8",
            _ => "application/octet-stream",
        }
    } else {
        "application/octet-stream"
    }
}

async fn serve_static_file(route_prefix: &str, dir: &str, file: &str) -> Response {
    let target = if file.is_empty() {
        "index.html".to_string()
    } else {
        file.to_string()
    };

    let Some(path) = safe_static_join(dir, &target) else {
        return Response {
            status: 400,
            content_type: "text/plain; charset=utf-8".into(),
            body: format!("Bad static file path under {route_prefix}").into_bytes(),
            headers: HashMap::new(),
            set_cookies: vec![],
        };
    };

    match tokio::fs::read(&path).await {
        Ok(bytes) => Response {
            status: 200,
            content_type: guess_content_type(&target).into(),
            body: bytes,
            headers: HashMap::new(),
            set_cookies: vec![],
        },
        Err(_) => Response {
            status: 404,
            content_type: "text/plain; charset=utf-8".into(),
            body: b"Not Found".to_vec(),
            headers: HashMap::new(),
            set_cookies: vec![],
        },
    }
}

async fn value_to_axum_response(v: Value) -> axum::response::Response {
    let r = match v {
        Value::Response(r) => r,
        Value::Json(j) => Response {
            status: 200,
            content_type: "application/json".into(),
            body: j.to_string().into_bytes(),
            headers: HashMap::new(),
            set_cookies: vec![],
        },
        Value::Dict(m) => {
            // dict/list => JSON
            let mut obj = serde_json::Map::new();
            for (k, vv) in m {
                obj.insert(k, J::String(vv.repr()));
            }
            Response {
                status: 200,
                content_type: "application/json".into(),
                body: J::Object(obj).to_string().into_bytes(),
                headers: HashMap::new(),
                set_cookies: vec![],
            }
        }
        Value::List(vs) => {
            let arr = vs
                .into_iter()
                .map(|x| J::String(x.repr()))
                .collect::<Vec<_>>();
            Response {
                status: 200,
                content_type: "application/json".into(),
                body: J::Array(arr).to_string().into_bytes(),
                headers: HashMap::new(),
                set_cookies: vec![],
            }
        }
        Value::Bytes(b) => Response {
            status: 200,
            content_type: "application/octet-stream".into(),
            body: b,
            headers: HashMap::new(),
            set_cookies: vec![],
        },
        Value::Str(s) => {
            // template rendering happens in handler; here pick content-type
            let ct = if s.contains("<") || s.contains("{{") {
                "text/html; charset=utf-8"
            } else {
                "text/plain; charset=utf-8"
            };
            Response {
                status: 200,
                content_type: ct.into(),
                body: s.into_bytes(),
                headers: HashMap::new(),
                set_cookies: vec![],
            }
        }
        other => Response {
            status: 200,
            content_type: "text/plain; charset=utf-8".into(),
            body: other.repr().into_bytes(),
            headers: HashMap::new(),
            set_cookies: vec![],
        },
    };

    {
        let mut resp = (
            StatusCode::from_u16(r.status).unwrap_or(StatusCode::OK),
            [(axum::http::header::CONTENT_TYPE, r.content_type)],
            r.body,
        )
            .into_response();
        for (k, v) in r.headers {
            if let (Ok(hn), Ok(hv)) = (
                axum::http::header::HeaderName::from_bytes(k.as_bytes()),
                axum::http::header::HeaderValue::from_str(&v),
            ) {
                resp.headers_mut().insert(hn, hv);
            }
        }
        for cookie in r.set_cookies {
            if let Ok(hv) = axum::http::header::HeaderValue::from_str(&cookie) {
                resp.headers_mut()
                    .append(axum::http::header::SET_COOKIE, hv);
            }
        }
        resp
    }
}

// ========================= HTTP Client =========================

#[derive(Clone)]
struct HttpHandle {
    client: reqwest::Client,
}
impl HttpHandle {
    fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
    fn get_member(&self, name: &str) -> Value {
        // bound method: (args, kwargs) -> Deferred<HttpResponse>
        let client = self.client.clone();
        match name {
            "get" => Value::Builtin(Arc::new(move |args, _| {
                let client = client.clone();
                Box::pin(async move {
                    let url = expect_str(&args, 0, "http.get(url)")?;
                    let d = Deferred::new(Box::pin(async move {
                        let resp = client
                            .get(url)
                            .send()
                            .await
                            .map_err(|e| RelayError::Runtime(e.to_string()))?;
                        let status = resp.status().as_u16();
                        let headers = resp
                            .headers()
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                            .collect::<HashMap<_, _>>();
                        let bytes = resp
                            .bytes()
                            .await
                            .map_err(|e| RelayError::Runtime(e.to_string()))?
                            .to_vec();
                        Ok(Value::Obj(Object::HttpResponse(HttpResponseHandle {
                            status,
                            headers,
                            body: bytes,
                        })))
                    }));
                    Ok(Value::Deferred(Arc::new(d)))
                })
            })),
            "post" => Value::Builtin(Arc::new(move |args, _| {
                let client = client.clone();
                Box::pin(async move {
                    let url = expect_str(&args, 0, "http.post(url, data)")?;
                    let data = args.get(1).cloned().unwrap_or(Value::None);
                    let d = Deferred::new(Box::pin(async move {
                        let req = client.post(url);
                        let req = match data {
                            Value::Json(j) => req.json(&j),
                            Value::Dict(m) => {
                                let mut obj = serde_json::Map::new();
                                for (k, vv) in m {
                                    obj.insert(k, J::String(vv.repr()));
                                }
                                req.json(&J::Object(obj))
                            }
                            Value::Str(s) => req.body(s),
                            Value::Bytes(b) => req.body(b),
                            other => req.body(other.repr()),
                        };
                        let resp = req
                            .send()
                            .await
                            .map_err(|e| RelayError::Runtime(e.to_string()))?;
                        let status = resp.status().as_u16();
                        let headers = resp
                            .headers()
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                            .collect::<HashMap<_, _>>();
                        let bytes = resp
                            .bytes()
                            .await
                            .map_err(|e| RelayError::Runtime(e.to_string()))?
                            .to_vec();
                        Ok(Value::Obj(Object::HttpResponse(HttpResponseHandle {
                            status,
                            headers,
                            body: bytes,
                        })))
                    }));
                    Ok(Value::Deferred(Arc::new(d)))
                })
            })),
            _ => Value::None,
        }
    }
}

#[derive(Clone)]
struct HttpResponseHandle {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}
impl HttpResponseHandle {
    fn get_member(&self, name: &str) -> Value {
        match name {
            "status" => Value::Int(self.status as i64),
            "text" => {
                let s = String::from_utf8_lossy(&self.body).to_string();
                Value::Str(s)
            }
            "json" => {
                let body = self.body.clone();
                Value::Builtin(Arc::new(move |_args, _| {
                    let body = body.clone();
                    Box::pin(async move {
                        let s = String::from_utf8_lossy(&body).to_string();
                        let j: J = serde_json::from_str(&s)
                            .map_err(|e| RelayError::Runtime(e.to_string()))?;
                        Ok(Value::Json(j))
                    })
                }))
            }
            _ => Value::None,
        }
    }
}

// ========================= MongoDB =========================

#[derive(Clone)]
struct MongoClientHandle {
    client: MongoClient,
}
#[derive(Clone)]
struct MongoDatabaseHandle {
    db: MongoDatabase,
}
#[derive(Clone)]
struct MongoCollectionHandle {
    collection: MongoCollection<Document>,
}

impl MongoClientHandle {
    fn new(client: MongoClient) -> Self {
        Self { client }
    }
    fn get_member(&self, name: &str) -> Value {
        match name {
            "db" => {
                let client = self.client.clone();
                Value::Builtin(Arc::new(move |args, _| {
                    let client = client.clone();
                    Box::pin(async move {
                        let name = expect_str(&args, 0, "mongo.db(name)")?;
                        Ok(Value::Obj(Object::MongoDatabase(MongoDatabaseHandle {
                            db: client.database(&name),
                        })))
                    })
                }))
            }
            _ => Value::None,
        }
    }
}

impl MongoDatabaseHandle {
    fn get_member(&self, name: &str) -> Value {
        match name {
            "collection" => {
                let db = self.db.clone();
                Value::Builtin(Arc::new(move |args, _| {
                    let db = db.clone();
                    Box::pin(async move {
                        let name = expect_str(&args, 0, "db.collection(name)")?;
                        Ok(Value::Obj(Object::MongoCollection(MongoCollectionHandle {
                            collection: db.collection::<Document>(&name),
                        })))
                    })
                }))
            }
            _ => Value::None,
        }
    }
}

impl MongoCollectionHandle {
    fn get_member(&self, name: &str) -> Value {
        match name {
            "insert_one" => {
                let collection = self.collection.clone();
                Value::Builtin(Arc::new(move |args, _| {
                    let collection = collection.clone();
                    Box::pin(async move {
                        let doc = value_to_document(
                            args.get(0).cloned().unwrap_or(Value::None),
                            "collection.insert_one(doc)",
                        )?;
                        let d = Deferred::new(Box::pin(async move {
                            let result = collection
                                .insert_one(doc, None)
                                .await
                                .map_err(|e| RelayError::Runtime(e.to_string()))?;
                            let mut obj = serde_json::Map::new();
                            obj.insert("inserted_id".into(), bson_to_json(result.inserted_id)?);
                            Ok(Value::Json(J::Object(obj)))
                        }));
                        Ok(Value::Deferred(Arc::new(d)))
                    })
                }))
            }
            "insert_many" => {
                let collection = self.collection.clone();
                Value::Builtin(Arc::new(move |args, _| {
                    let collection = collection.clone();
                    Box::pin(async move {
                        let list = match args.get(0) {
                            Some(Value::List(v)) => v.clone(),
                            _ => {
                                return Err(RelayError::Type(
                                    "collection.insert_many(docs) expects a list".into(),
                                ))
                            }
                        };
                        let mut docs = Vec::with_capacity(list.len());
                        for v in list {
                            docs.push(value_to_document(v, "collection.insert_many(docs)")?);
                        }
                        let d = Deferred::new(Box::pin(async move {
                            let result = collection
                                .insert_many(docs, None)
                                .await
                                .map_err(|e| RelayError::Runtime(e.to_string()))?;
                            let mut obj = serde_json::Map::new();
                            let mut ids = serde_json::Map::new();
                            for (k, v) in result.inserted_ids {
                                ids.insert(k.to_string(), bson_to_json(v)?);
                            }
                            obj.insert("inserted_ids".into(), J::Object(ids));
                            Ok(Value::Json(J::Object(obj)))
                        }));
                        Ok(Value::Deferred(Arc::new(d)))
                    })
                }))
            }
            "find_one" => {
                let collection = self.collection.clone();
                Value::Builtin(Arc::new(move |args, _| {
                    let collection = collection.clone();
                    Box::pin(async move {
                        let filter = value_to_document(
                            args.get(0).cloned().unwrap_or(Value::Dict(IndexMap::new())),
                            "collection.find_one(filter)",
                        )?;
                        let d = Deferred::new(Box::pin(async move {
                            let result = collection
                                .find_one(filter, None)
                                .await
                                .map_err(|e| RelayError::Runtime(e.to_string()))?;
                            match result {
                                Some(doc) => Ok(Value::Json(bson_to_json(Bson::Document(doc))?)),
                                None => Ok(Value::None),
                            }
                        }));
                        Ok(Value::Deferred(Arc::new(d)))
                    })
                }))
            }
            "find" => {
                let collection = self.collection.clone();
                Value::Builtin(Arc::new(move |args, _| {
                    let collection = collection.clone();
                    Box::pin(async move {
                        let filter = value_to_document(
                            args.get(0).cloned().unwrap_or(Value::Dict(IndexMap::new())),
                            "collection.find(filter)",
                        )?;
                        let d = Deferred::new(Box::pin(async move {
                            let mut cursor = collection
                                .find(filter, None)
                                .await
                                .map_err(|e| RelayError::Runtime(e.to_string()))?;
                            let mut out = Vec::new();
                            while let Some(doc) = cursor
                                .try_next()
                                .await
                                .map_err(|e| RelayError::Runtime(e.to_string()))?
                            {
                                out.push(Value::Json(bson_to_json(Bson::Document(doc))?));
                            }
                            Ok(Value::List(out))
                        }));
                        Ok(Value::Deferred(Arc::new(d)))
                    })
                }))
            }
            "update_one" => {
                let collection = self.collection.clone();
                Value::Builtin(Arc::new(move |args, _| {
                    let collection = collection.clone();
                    Box::pin(async move {
                        if args.len() < 2 {
                            return Err(RelayError::Type(
                                "collection.update_one(filter, update)".into(),
                            ));
                        }
                        let filter = value_to_document(
                            args[0].clone(),
                            "collection.update_one(filter, update)",
                        )?;
                        let update = value_to_document(
                            args[1].clone(),
                            "collection.update_one(filter, update)",
                        )?;
                        let d = Deferred::new(Box::pin(async move {
                            let result = collection
                                .update_one(filter, update, None)
                                .await
                                .map_err(|e| RelayError::Runtime(e.to_string()))?;
                            let mut obj = serde_json::Map::new();
                            obj.insert(
                                "matched_count".into(),
                                J::Number(result.matched_count.into()),
                            );
                            obj.insert(
                                "modified_count".into(),
                                J::Number(result.modified_count.into()),
                            );
                            if let Some(id) = result.upserted_id {
                                obj.insert("upserted_id".into(), bson_to_json(id)?);
                            }
                            Ok(Value::Json(J::Object(obj)))
                        }));
                        Ok(Value::Deferred(Arc::new(d)))
                    })
                }))
            }
            "delete_one" => {
                let collection = self.collection.clone();
                Value::Builtin(Arc::new(move |args, _| {
                    let collection = collection.clone();
                    Box::pin(async move {
                        let filter = value_to_document(
                            args.get(0).cloned().unwrap_or(Value::Dict(IndexMap::new())),
                            "collection.delete_one(filter)",
                        )?;
                        let d = Deferred::new(Box::pin(async move {
                            let result = collection
                                .delete_one(filter, None)
                                .await
                                .map_err(|e| RelayError::Runtime(e.to_string()))?;
                            Ok(Value::Int(result.deleted_count as i64))
                        }));
                        Ok(Value::Deferred(Arc::new(d)))
                    })
                }))
            }
            "delete_many" => {
                let collection = self.collection.clone();
                Value::Builtin(Arc::new(move |args, _| {
                    let collection = collection.clone();
                    Box::pin(async move {
                        let filter = value_to_document(
                            args.get(0).cloned().unwrap_or(Value::Dict(IndexMap::new())),
                            "collection.delete_many(filter)",
                        )?;
                        let d = Deferred::new(Box::pin(async move {
                            let result = collection
                                .delete_many(filter, None)
                                .await
                                .map_err(|e| RelayError::Runtime(e.to_string()))?;
                            Ok(Value::Int(result.deleted_count as i64))
                        }));
                        Ok(Value::Deferred(Arc::new(d)))
                    })
                }))
            }
            _ => Value::None,
        }
    }
}

// ========================= Object dispatch (member + call) =========================

impl Object {
    fn get_member(&self, name: &str) -> RResult<Value> {
        Ok(match self {
            Object::WebApp(a) => match name {
                "route" => {
                    // app.route() -> Route object
                    let a = a.clone();
                    Value::Builtin(Arc::new(move |_args, _| {
                        let a = a.clone();
                        Box::pin(async move { Ok(Value::Obj(Object::Route(a.route()))) })
                    }))
                }
                "redirect" => Value::Builtin(Arc::new(move |args, _| {
                    Box::pin(async move {
                        let location = expect_str(&args, 0, "app.redirect(location)")?;
                        let mut headers = HashMap::new();
                        headers.insert("location".into(), location);
                        Ok(Value::Response(Response {
                            status: 302,
                            content_type: "text/plain; charset=utf-8".into(),
                            body: Vec::new(),
                            headers,
                            set_cookies: vec![],
                        }))
                    })
                })),
                "json" => Value::Builtin(Arc::new(move |args, _| {
                    Box::pin(async move {
                        let body = args.get(0).cloned().unwrap_or(Value::None);
                        Ok(Value::Response(Response {
                            status: 200,
                            content_type: "application/json".into(),
                            body: value_to_json(&body).to_string().into_bytes(),
                            headers: HashMap::new(),
                            set_cookies: vec![],
                        }))
                    })
                })),
                "use" => {
                    let a = a.clone();
                    Value::Builtin(Arc::new(move |args, _| {
                        let a = a.clone();
                        Box::pin(async move {
                            let middleware_fn = args.get(0).cloned().ok_or_else(|| {
                                RelayError::Type("app.use(fn) expects a function".into())
                            })?;
                            let fn_name = match middleware_fn {
                                Value::Function(f) => f.name.clone(),
                                _ => {
                                    return Err(RelayError::Type(
                                        "app.use(fn) expects a function".into(),
                                    ))
                                }
                            };
                            a.use_middleware(fn_name);
                            Ok(Value::None)
                        })
                    }))
                }
                "static" => {
                    let a = a.clone();
                    Value::Builtin(Arc::new(move |args, _| {
                        let a = a.clone();
                        Box::pin(async move {
                            let route_prefix = expect_str(&args, 0, "app.static(route, dir)")?;
                            let dir = expect_str(&args, 1, "app.static(route, dir)")?;
                            a.mount_static(route_prefix, dir);
                            Ok(Value::None)
                        })
                    }))
                }
                _ => Value::None,
            },
            Object::Route(_r) => Value::None, // decorators handle route registration
            Object::WebServer(s) => match name {
                "run" => {
                    let s = s.clone();
                    Value::Builtin(Arc::new(move |args, _| {
                        let s = s.clone();
                        Box::pin(async move {
                            let app = match args.get(0) {
                                Some(Value::Obj(Object::WebApp(a))) => a.clone(),
                                _ => {
                                    return Err(RelayError::Type(
                                        "server.run(app) expects a WebApp".into(),
                                    ))
                                }
                            };
                            s.run(app).await
                        })
                    }))
                }
                _ => Value::None,
            },
            Object::Http(h) => h.get_member(name),
            Object::HttpResponse(r) => r.get_member(name),
            Object::MongoClient(m) => m.get_member(name),
            Object::MongoDatabase(d) => d.get_member(name),
            Object::MongoCollection(c) => c.get_member(name),
        })
    }

    fn call(&self, _ev: &Evaluator, _args: Vec<Value>, _kwargs: Vec<(String, Value)>) -> BoxFut {
        Box::pin(async move { Err(RelayError::Type("Object is not directly callable".into())) })
    }
}

impl WebServerHandle {
    async fn run(&self, app: WebAppHandle) -> RResult<Value> {
        let d = Deferred::new(Box::pin(async move {
            run_app(app).await?;
            Ok(Value::None)
        }));
        Ok(Value::Deferred(Arc::new(d)))
    }
}

// ========================= Web handler injection + smart returns =========================

impl Evaluator {
    async fn call_web_handler(
        &self,
        app: WebAppHandle,
        fn_name: &str,
        req: RequestParts,
    ) -> RResult<Value> {
        // Path > Body > Query precedence, and "data: Json" style typing
        let f = { self.env.lock().await.get(fn_name)? };
        let f = self.resolve_if_needed(f).await?;

        let func = match f {
            Value::Function(f) => f,
            _ => {
                return Err(RelayError::Type(format!(
                    "Handler {fn_name} is not a function"
                )))
            }
        };

        let RequestParts {
            method,
            path,
            path_params,
            query,
            json,
            headers,
            cookies,
            session_id,
        } = req;

        let query_map = query.clone();
        let json_body = json.clone();

        // build arg map by param list
        let mut bound: HashMap<String, Value> = HashMap::new();

        // start with Query
        for (k, v) in query {
            bound.insert(k, Value::Str(v));
        }
        // then Body
        if let Some(j) = json {
            // if handler has a Json param name, bind it (first param typed Json)
            let mut json_param_name = None;
            for p in &func.params {
                if matches!(p.ty.as_deref(), Some("Json") | Some("json")) {
                    json_param_name = Some(p.name.clone());
                    break;
                }
            }
            if let Some(n) = json_param_name {
                bound.insert(n, Value::Json(j.clone()));
            } else {
                // default: "data"
                bound.insert("data".into(), Value::Json(j.clone()));
            }
        }
        // then Path overrides
        for (k, v) in path_params {
            bound.insert(k, Value::Str(v));
        }

        // apply defaults for missing
        for p in &func.params {
            if !bound.contains_key(&p.name) {
                if let Some(def) = &p.default {
                    bound.insert(
                        p.name.clone(),
                        self.resolve_if_needed(self.eval_expr(def).await?).await?,
                    );
                } else {
                    bound.insert(p.name.clone(), Value::None);
                }
            }
        }

        // type coercion
        for p in &func.params {
            if let Some(ty) = &p.ty {
                let v = bound.get(&p.name).cloned().unwrap_or(Value::None);
                bound.insert(p.name.clone(), coerce_param_type(ty, v)?);
            }
        }

        let mut req_dict = IndexMap::new();
        req_dict.insert("method".into(), Value::Str(method));
        req_dict.insert("path".into(), Value::Str(path));
        req_dict.insert(
            "query".into(),
            Value::Dict(
                query_map
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
                    .collect(),
            ),
        );
        req_dict.insert(
            "headers".into(),
            Value::Dict(
                headers
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
                    .collect(),
            ),
        );
        req_dict.insert(
            "cookies".into(),
            Value::Dict(
                cookies
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
                    .collect(),
            ),
        );
        if let Some(j) = json_body {
            req_dict.insert("json".into(), Value::Json(j));
        }

        let existing_session = session_id
            .as_ref()
            .map(|sid| app.get_session(sid))
            .unwrap_or_default();

        // run handler in new scope
        {
            let mut env = self.env.lock().await;
            env.push();
            for (k, v) in &bound {
                env.set(k, v.clone());
            }
            env.set("request", Value::Dict(req_dict));
            env.set(
                "cookies",
                Value::Dict(
                    cookies
                        .iter()
                        .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
                        .collect(),
                ),
            );
            env.set("session", Value::Dict(existing_session.clone()));
        }

        for mw_name in app.middleware_names() {
            let mw_value = self.env.lock().await.get(&mw_name)?;
            let mw_func = match mw_value {
                Value::Function(f) => f,
                _ => {
                    return Err(RelayError::Type(format!(
                        "Middleware '{mw_name}' is not a function"
                    )))
                }
            };

            let result = self.eval_block(&mw_func.body).await?;
            if let Flow::Return(v) = result {
                if !matches!(v, Value::None) {
                    let final_session = {
                        let env = self.env.lock().await;
                        let locals = env.snapshot_top();
                        match locals.get("session") {
                            Some(Value::Dict(m)) => m.clone(),
                            _ => existing_session.clone(),
                        }
                    };

                    {
                        let mut env = self.env.lock().await;
                        env.pop();
                    }

                    return Ok(apply_session_and_cookies(app, session_id, final_session, v));
                }
            }
        }

        let out = match self.eval_block(&func.body).await? {
            Flow::None => Value::None,
            Flow::Return(v) => v,
        };

        let final_session = {
            let env = self.env.lock().await;
            let locals = env.snapshot_top();
            match locals.get("session") {
                Some(Value::Dict(m)) => m.clone(),
                _ => existing_session,
            }
        };

        {
            let mut env = self.env.lock().await;
            env.pop();
        }

        let mut response = match out {
            Value::Response(r) => r,
            Value::Json(j) => Response {
                status: 200,
                content_type: "application/json".into(),
                body: j.to_string().into_bytes(),
                headers: HashMap::new(),
                set_cookies: vec![],
            },
            Value::Dict(m) => {
                let mut obj = serde_json::Map::new();
                for (k, vv) in m {
                    obj.insert(k, J::String(vv.repr()));
                }
                Response {
                    status: 200,
                    content_type: "application/json".into(),
                    body: J::Object(obj).to_string().into_bytes(),
                    headers: HashMap::new(),
                    set_cookies: vec![],
                }
            }
            Value::List(vs) => {
                let arr = vs
                    .into_iter()
                    .map(|x| J::String(x.repr()))
                    .collect::<Vec<_>>();
                Response {
                    status: 200,
                    content_type: "application/json".into(),
                    body: J::Array(arr).to_string().into_bytes(),
                    headers: HashMap::new(),
                    set_cookies: vec![],
                }
            }
            Value::Bytes(b) => Response {
                status: 200,
                content_type: "application/octet-stream".into(),
                body: b,
                headers: HashMap::new(),
                set_cookies: vec![],
            },
            Value::Str(s) => {
                let ct = if s.contains("<") || s.contains("{{") {
                    "text/html; charset=utf-8"
                } else {
                    "text/plain; charset=utf-8"
                };
                Response {
                    status: 200,
                    content_type: ct.into(),
                    body: s.into_bytes(),
                    headers: HashMap::new(),
                    set_cookies: vec![],
                }
            }
            other => Response {
                status: 200,
                content_type: "text/plain; charset=utf-8".into(),
                body: other.repr().into_bytes(),
                headers: HashMap::new(),
                set_cookies: vec![],
            },
        };

        if !final_session.is_empty() {
            let sid = session_id.unwrap_or_else(new_session_id);
            app.put_session(sid.clone(), final_session);
            response
                .set_cookies
                .push(format!("relay_sid={sid}; Path=/; HttpOnly; SameSite=Lax"));
        }

        Ok(Value::Response(response))
    }
}

fn apply_session_and_cookies(
    app: WebAppHandle,
    session_id: Option<String>,
    final_session: IndexMap<String, Value>,
    out: Value,
) -> Value {
    let mut response = match out {
        Value::Response(r) => r,
        Value::Json(j) => Response {
            status: 200,
            content_type: "application/json".into(),
            body: j.to_string().into_bytes(),
            headers: HashMap::new(),
            set_cookies: vec![],
        },
        Value::Dict(m) => {
            let mut obj = serde_json::Map::new();
            for (k, vv) in m {
                obj.insert(k, J::String(vv.repr()));
            }
            Response {
                status: 200,
                content_type: "application/json".into(),
                body: J::Object(obj).to_string().into_bytes(),
                headers: HashMap::new(),
                set_cookies: vec![],
            }
        }
        Value::List(vs) => {
            let arr = vs
                .into_iter()
                .map(|x| J::String(x.repr()))
                .collect::<Vec<_>>();
            Response {
                status: 200,
                content_type: "application/json".into(),
                body: J::Array(arr).to_string().into_bytes(),
                headers: HashMap::new(),
                set_cookies: vec![],
            }
        }
        Value::Bytes(b) => Response {
            status: 200,
            content_type: "application/octet-stream".into(),
            body: b,
            headers: HashMap::new(),
            set_cookies: vec![],
        },
        Value::Str(s) => {
            let ct = if s.contains("<") || s.contains("{{") {
                "text/html; charset=utf-8"
            } else {
                "text/plain; charset=utf-8"
            };
            Response {
                status: 200,
                content_type: ct.into(),
                body: s.into_bytes(),
                headers: HashMap::new(),
                set_cookies: vec![],
            }
        }
        other => Response {
            status: 200,
            content_type: "text/plain; charset=utf-8".into(),
            body: other.repr().into_bytes(),
            headers: HashMap::new(),
            set_cookies: vec![],
        },
    };

    if !final_session.is_empty() {
        let sid = session_id.unwrap_or_else(new_session_id);
        app.put_session(sid.clone(), final_session);
        response
            .set_cookies
            .push(format!("relay_sid={sid}; Path=/; HttpOnly; SameSite=Lax"));
    }

    Value::Response(response)
}

// ========================= Main =========================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap_or_else(|| {
        eprintln!("Usage: relay <file.ry>");
        std::process::exit(2);
    });

    let src = tokio::fs::read_to_string(&path).await?;
    let mut lexer = Lexer::new(&src);
    let tokens = lexer.tokenize()?;

    let mut parser = Parser::new(tokens);
    let program = parser.parse_program()?;
    let entry_path = tokio::fs::canonicalize(&path).await?;

    let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
    let evaluator = Arc::new(Evaluator::new(env.clone()));

    {
        let mut env_lock = env.lock().await;
        install_stdlib(&mut env_lock, evaluator.clone()).map_err(anyhow::Error::msg)?;

        // Convenience: if user follows canonical example, they set:
        // server = WebServer(); app = WebApp(); route = app.route()
        // That route var is then used in decorators.
    }

    // run program (this defines functions, registers decorators if route exists before defs)
    let result = evaluator
        .eval_program_in_file(&program, entry_path)
        .await
        .map_err(anyhow::Error::msg)?;

    // Wait for any background Deferred/Task spawned by expression statements
    drain_bg().await;

    // If user defined functions with decorators before creating route variable,
    // they can also do route registration later by re-running a pass.
    // (v0.1: simplest rule is "create route before handler defs".)

    if !result.is_none() {
        println!("{}", result.repr());
    }
    Ok(())
}
