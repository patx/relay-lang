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
    body::Bytes,
    extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
    extract::{Path, Query},
    http::{HeaderMap, Method, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};

use async_recursion::async_recursion;
use argon2::{
    password_hash::{SaltString, rand_core::OsRng},
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
};
use futures::{stream::TryStreamExt, SinkExt, StreamExt};
use indexmap::IndexMap;
use lettre::{
    message::{
        header as mail_header, Attachment as MailAttachment, Mailbox, Message as MailMessage,
        MultiPart, SinglePart,
    },
    transport::smtp::authentication::Credentials,
    AsyncSmtpTransport, AsyncTransport, Tokio1Executor,
};
use minijinja::Environment;
use mongodb::{
    bson::{self, oid::ObjectId, Bson, Document},
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
        target: Expr,
        expr: Expr,
    },
    AugAssign {
        target: Expr,
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
        validate_schema: Option<Expr>,
        query_schema: Option<Expr>,
        body_schema: Option<Expr>,
        json_schema: Option<Expr>,
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
    And,
    Or,
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
    grouping_depth: usize,
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
            grouping_depth: 0,
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

                // Inside (), [] and {}, indentation/newlines are ignored for implicit line joining.
                if self.grouping_depth > 0 {
                    self.at_line_start = false;
                } else if self.peek_is_newline() || self.peek_is_comment_start() {
                    // blank line or comment-only line => ignore indentation
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
                    self.at_line_start = false;
                }
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
                self.at_line_start = true;
                if self.grouping_depth == 0 {
                    out.push(self.tok(Tok::Newline));
                }
                continue;
            }

            let kind = match c {
                '(' => {
                    self.advance_char();
                    self.grouping_depth += 1;
                    Tok::LParen
                }
                ')' => {
                    self.advance_char();
                    if self.grouping_depth > 0 {
                        self.grouping_depth -= 1;
                    }
                    Tok::RParen
                }
                '[' => {
                    self.advance_char();
                    self.grouping_depth += 1;
                    Tok::LBracket
                }
                ']' => {
                    self.advance_char();
                    if self.grouping_depth > 0 {
                        self.grouping_depth -= 1;
                    }
                    Tok::RBracket
                }
                '{' => {
                    self.advance_char();
                    self.grouping_depth += 1;
                    Tok::LBrace
                }
                '}' => {
                    self.advance_char();
                    if self.grouping_depth > 0 {
                        self.grouping_depth -= 1;
                    }
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
                '&' => {
                    if self.peek2("&&") {
                        self.advance_n(2);
                        Tok::Op("&&".into())
                    } else {
                        return Err(self.err("Unexpected '&'"));
                    }
                }
                '|' => {
                    if self.peek2("||") {
                        self.advance_n(2);
                        Tok::Op("||".into())
                    } else {
                        return Err(self.err("Unexpected '|'"));
                    }
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
                            "fn" | "if" | "else" | "for" | "while" | "return" | "try"
                            | "except" | "import" | "True" | "False" | "None" | "str" | "int"
                            | "float" => Tok::Keyword(id),
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

    fn count_leading_ws(&self) -> (usize, bool) {
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

        if self.peek_else() {
            return Err(
                self.err_here("'else' without matching if (check indentation and block structure)")
            );
        }

        if self.peek_destructure_assign() {
            return self.parse_destructure_assign();
        }

        let target = self.parse_expr()?;
        if self.peek_is(&Tok::Assign) || self.peek_is(&Tok::AugAdd) || self.peek_is(&Tok::AugSub)
        {
            if !Self::is_assignment_target(&target) {
                return Err(self.err_here("Invalid assignment target"));
            }
            let op = self.bump().kind.clone();
            let expr = self.parse_expr()?;
            return Ok(match op {
                Tok::Assign => Stmt::Assign { target, expr },
                Tok::AugAdd => Stmt::AugAssign {
                    target,
                    op: AugOp::Add,
                    expr,
                },
                Tok::AugSub => Stmt::AugAssign {
                    target,
                    op: AugOp::Sub,
                    expr,
                },
                _ => unreachable!(),
            });
        }

        Ok(Stmt::Expr(target))
    }

    fn is_assignment_target(expr: &Expr) -> bool {
        match expr {
            Expr::Ident(_) => true,
            Expr::Member { object, .. } => Self::is_assignment_target(object),
            Expr::Index { target, .. } => Self::is_assignment_target(target),
            _ => false,
        }
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
        let mut validate_schema = None;
        let mut query_schema = None;
        let mut body_schema = None;
        let mut json_schema = None;

        if self.peek_is(&Tok::Comma) {
            self.bump();
            while !self.peek_is(&Tok::RParen) {
                let key = self.expect_ident()?;
                self.expect(&Tok::Assign)?;
                let value = self.parse_expr()?;
                match key.as_str() {
                    "validate" => validate_schema = Some(value),
                    "query" => query_schema = Some(value),
                    "body" | "form" => body_schema = Some(value),
                    "json" => json_schema = Some(value),
                    _ => {
                        return Err(self.err_here(
                            "Decorator option must be one of: validate, query, body, json",
                        ))
                    }
                }

                if self.peek_is(&Tok::Comma) {
                    self.bump();
                    if self.peek_is(&Tok::RParen) {
                        break;
                    }
                    continue;
                }
                break;
            }
        }
        self.expect(&Tok::RParen)?;
        Ok(Decorator::Route {
            base,
            method,
            path,
            validate_schema,
            query_schema,
            body_schema,
            json_schema,
        })
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
        if self.peek_else() {
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
                "&&" => (BinOp::And, 1),
                "||" => (BinOp::Or, 0),
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
        } else if let Some(id) = self.peek_ident_string() {
            match id.as_str() {
                "and" => Some((BinOp::And, 1)),
                "or" => Some((BinOp::Or, 0)),
                _ => None,
            }
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
    fn peek_else(&self) -> bool {
        self.peek_kw("else") || self.peek_ident_string().as_deref() == Some("else")
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
    Email(EmailHandle),
    AuthStore(AuthStoreHandle),
    MongoClient(MongoClientHandle),
    MongoDatabase(MongoDatabaseHandle),
    MongoCollection(MongoCollectionHandle),
    WebSocket(WebSocketHandle),
}

#[derive(Clone)]
struct WebSocketHandle {
    inner: Arc<tokio::sync::Mutex<WebSocket>>,
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

fn render_template(s: &str, locals: &HashMap<String, Value>) -> RResult<String> {
    let mut ctx = serde_json::Map::new();
    for (key, value) in locals {
        ctx.insert(key.clone(), value_to_json(value));
    }

    let ctx = J::Object(ctx);

    Environment::new()
        .render_str(s, &ctx)
        .map_err(|e| RelayError::Runtime(format!("Template render error: {e}")))
}

// ========================= Evaluator (async, implicit Deferred) =========================

#[derive(Clone)]
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
            Stmt::Assign { target, expr } => {
                let v = self.eval_expr(expr).await?;
                self.assign_to_expr(target, v).await?;
                Ok(Flow::None)
            }
            Stmt::AugAssign { target, op, expr } => {
                let rhs = self
                    .resolve_deferred_only(self.eval_expr(expr).await?)
                    .await?;
                let cur = self.eval_expr(target).await?;
                let cur = self.resolve_if_needed(cur).await?;
                let out = match op {
                    AugOp::Add => bin_add(cur, rhs)?,
                    AugOp::Sub => bin_sub(cur, rhs)?,
                };
                self.assign_to_expr(target, out).await?;
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
    async fn assign_to_expr(&self, target_expr: &Expr, value: Value) -> RResult<()> {
        match target_expr {
            Expr::Ident(name) => {
                let mut env = self.env.lock().await;
                if !env.assign_existing(name, value.clone()) {
                    env.set(name, value);
                }
                Ok(())
            }
            Expr::Member { object, name } => {
                let object_value = self
                    .resolve_if_needed(self.eval_expr(object).await?)
                    .await?;
                let updated = assign_member_value(object_value, name, value)?;
                self.assign_to_expr(object, updated).await
            }
            Expr::Index { target, index } => {
                let container = self
                    .resolve_if_needed(self.eval_expr(target).await?)
                    .await?;
                let idx = self.resolve_if_needed(self.eval_expr(index).await?).await?;
                let updated = assign_index_value(container, idx, value)?;
                self.assign_to_expr(target, updated).await
            }
            _ => Err(RelayError::Type("Invalid assignment target".into())),
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
                    Ok(Value::Str(render_template(s, &locals)?))
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
                    (Value::Dict(m), k) => Ok(m.get(&k.repr()).cloned().unwrap_or(Value::None)),
                    (Value::Json(j), Value::Str(k)) => {
                        Ok(j.get(&k).cloned().map(Value::Json).unwrap_or(Value::None))
                    }
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
                let is_explicit_template_method = matches!(
                    &**callee,
                    Expr::Member { name, .. } if name == "render_template" || name == "render"
                );

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
                    let v = if is_explicit_template_method && idx == 0 {
                        match x {
                            Expr::Str(s) => Value::Str(s.clone()),
                            _ => self.eval_expr(x).await?,
                        }
                    } else {
                        self.eval_expr(x).await?
                    };
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
                    let v = if is_explicit_template_method && k == "template" {
                        match x {
                            Expr::Str(s) => Value::Str(s.clone()),
                            _ => self.eval_expr(x).await?,
                        }
                    } else {
                        self.eval_expr(x).await?
                    };
                    let v = if is_spawn_ident {
                        v
                    } else {
                        self.resolve_deferred_only(v).await?
                    };
                    kw.push((k.clone(), v));
                }

                self.call_value(c, a, kw).await
            }

            Expr::BinOp { left, op, right } => match op {
                BinOp::And => {
                    let l = self.resolve_if_needed(self.eval_expr(left).await?).await?;
                    if !l.truthy() {
                        Ok(Value::Bool(false))
                    } else {
                        let r = self.resolve_if_needed(self.eval_expr(right).await?).await?;
                        Ok(Value::Bool(r.truthy()))
                    }
                }
                BinOp::Or => {
                    let l = self.resolve_if_needed(self.eval_expr(left).await?).await?;
                    if l.truthy() {
                        Ok(Value::Bool(true))
                    } else {
                        let r = self.resolve_if_needed(self.eval_expr(right).await?).await?;
                        Ok(Value::Bool(r.truthy()))
                    }
                }
                _ => {
                    let l = self.resolve_if_needed(self.eval_expr(left).await?).await?;
                    let r = self.resolve_if_needed(self.eval_expr(right).await?).await?;
                    eval_bin(*op, l, r)
                }
            },

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

        // type hints: strict runtime validation (no implicit coercion)
        for p in &f.params {
            if let Some(ty) = &p.ty {
                if let Some(v) = bound.get(&p.name).cloned() {
                    bound.insert(p.name.clone(), validate_param_type(ty, v)?);
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

    async fn call_named_function(&self, name: &str, args: Vec<Value>) -> RResult<Value> {
        let callee = { self.env.lock().await.get(name)? };
        let out = self.call_value(callee, args, vec![]).await?;
        self.resolve_if_needed(out).await
    }

    async fn load_session_data(
        &self,
        app: &WebAppHandle,
        session_id: &Option<String>,
    ) -> RResult<IndexMap<String, Value>> {
        let Some(sid) = session_id.clone() else {
            return Ok(IndexMap::new());
        };

        match app.session_backend() {
            SessionBackendConfig::Memory => Ok(app.get_session(&sid)),
            SessionBackendConfig::Callback { load_fn, .. } => {
                let out = self
                    .call_named_function(&load_fn, vec![Value::Str(sid)])
                    .await?;
                match out {
                    Value::None => Ok(IndexMap::new()),
                    Value::Dict(m) => Ok(m),
                    Value::Json(J::Object(obj)) => Ok(obj
                        .iter()
                        .map(|(k, v)| (k.clone(), json_to_value(v)))
                        .collect()),
                    other => Err(RelayError::Type(format!(
                        "session load callback must return dict/json/None, got {}",
                        value_type_name(&other)
                    ))),
                }
            }
        }
    }

    async fn persist_session_data(
        &self,
        app: &WebAppHandle,
        session_id: String,
        data: IndexMap<String, Value>,
    ) -> RResult<()> {
        match app.session_backend() {
            SessionBackendConfig::Memory => {
                app.put_session(session_id, data);
                Ok(())
            }
            SessionBackendConfig::Callback { save_fn, .. } => {
                let _ = self
                    .call_named_function(&save_fn, vec![Value::Str(session_id), Value::Dict(data)])
                    .await?;
                Ok(())
            }
        }
    }

    #[async_recursion]
    async fn run_middleware_chain(
        self: Arc<Self>,
        middlewares: Arc<Vec<Arc<UserFunction>>>,
        index: usize,
        ctx: Value,
        handler: Arc<dyn Fn() -> BoxFut + Send + Sync>,
    ) -> RResult<Value> {
        if index >= middlewares.len() {
            return (handler)().await;
        }

        let mw = middlewares[index].clone();
        let mut args = Vec::new();
        if !mw.params.is_empty() {
            args.push(ctx.clone());
        }

        let next_result = Arc::new(tokio::sync::Mutex::new(None::<Value>));
        if mw.params.len() >= 2 {
            let evaluator = self.clone();
            let middlewares = middlewares.clone();
            let handler = handler.clone();
            let ctx_for_next = ctx.clone();
            let next_result_cell = next_result.clone();
            args.push(Value::Builtin(Arc::new(move |_args, _kwargs| {
                let evaluator = evaluator.clone();
                let middlewares = middlewares.clone();
                let handler = handler.clone();
                let ctx_for_next = ctx_for_next.clone();
                let next_result_cell = next_result_cell.clone();
                Box::pin(async move {
                    let out = evaluator
                        .run_middleware_chain(middlewares, index + 1, ctx_for_next, handler)
                        .await?;
                    {
                        let mut cell = next_result_cell.lock().await;
                        *cell = Some(out.clone());
                    }
                    Ok(out)
                })
            })));
        }

        let result = self.call_user_fn(&mw, args, vec![]).await?;
        if !matches!(result, Value::None) {
            return Ok(result);
        }

        if mw.params.len() >= 2 {
            let from_next = next_result.lock().await.clone();
            return Ok(from_next.unwrap_or(Value::None));
        }

        self.run_middleware_chain(middlewares, index + 1, ctx, handler)
            .await
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
                Decorator::Route {
                    base,
                    method,
                    path,
                    validate_schema,
                    query_schema,
                    body_schema,
                    json_schema,
                } => {
                    let mut validation = RouteValidation::default();
                    if let Some(expr) = validate_schema {
                        validation.validate_schema =
                            Some(self.resolve_if_needed(self.eval_expr(expr).await?).await?);
                    }
                    if let Some(expr) = query_schema {
                        validation.query_schema =
                            Some(self.resolve_if_needed(self.eval_expr(expr).await?).await?);
                    }
                    if let Some(expr) = body_schema {
                        validation.body_schema =
                            Some(self.resolve_if_needed(self.eval_expr(expr).await?).await?);
                    }
                    if let Some(expr) = json_schema {
                        validation.json_schema =
                            Some(self.resolve_if_needed(self.eval_expr(expr).await?).await?);
                    }

                    let base_val = self.env.lock().await.get(base)?;
                    let base_val = self.resolve_if_needed(base_val).await?;
                    match base_val {
                        // New spec: @app.get("/path") where `app` is a WebApp
                        Value::Obj(Object::WebApp(a)) => {
                            a.register(
                                method.clone(),
                                path.clone(),
                                fn_name.to_string(),
                                validation.clone(),
                            );
                        }
                        // Back-compat: route = app.route(); @route.get("/path")
                        Value::Obj(Object::Route(r)) => {
                            r.register(
                                method.clone(),
                                path.clone(),
                                fn_name.to_string(),
                                validation.clone(),
                            );
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

fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Int(_) => "int",
        Value::Float(_) => "float",
        Value::Bool(_) => "bool",
        Value::Str(_) => "str",
        Value::None => "None",
        Value::List(_) => "list",
        Value::Dict(_) => "dict",
        Value::Json(_) => "Json",
        Value::Bytes(_) => "bytes",
        Value::Function(_) => "function",
        Value::Builtin(_) => "builtin",
        Value::Deferred(_) => "Deferred",
        Value::Task(_) => "Task",
        Value::Thunk(_) => "Thunk",
        Value::Obj(_) => "object",
        Value::Response(_) => "Response",
    }
}

fn validate_param_type(ty: &str, v: Value) -> RResult<Value> {
    match ty {
        "str" => match v {
            Value::Str(s) => Ok(Value::Str(s)),
            other => Err(RelayError::Type(format!(
                "Expected str, got {}",
                value_type_name(&other)
            ))),
        },
        "int" => match v {
            Value::Int(n) => Ok(Value::Int(n)),
            other => Err(RelayError::Type(format!(
                "Expected int, got {}",
                value_type_name(&other)
            ))),
        },
        "float" => match v {
            Value::Float(f) => Ok(Value::Float(f)),
            other => Err(RelayError::Type(format!(
                "Expected float, got {}",
                value_type_name(&other)
            ))),
        },
        "bool" => match v {
            Value::Bool(b) => Ok(Value::Bool(b)),
            other => Err(RelayError::Type(format!(
                "Expected bool, got {}",
                value_type_name(&other)
            ))),
        },
        "Json" | "json" => match v {
            Value::Json(j) => Ok(Value::Json(j)),
            Value::Dict(m) => {
                let mut obj = serde_json::Map::new();
                for (k, vv) in m {
                    obj.insert(k, value_to_json(&vv));
                }
                Ok(Value::Json(J::Object(obj)))
            }
            other => Err(RelayError::Type(format!(
                "Expected Json, got {}",
                value_type_name(&other)
            ))),
        },
        _ => Err(RelayError::Type(format!("Unknown type hint: {ty}"))),
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
        "bool" => match v {
            Value::Bool(b) => Ok(Value::Bool(b)),
            Value::Int(n) => Ok(Value::Bool(n != 0)),
            Value::Str(s) => match s.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => Ok(Value::Bool(true)),
                "false" | "0" | "no" | "off" => Ok(Value::Bool(false)),
                _ => Err(RelayError::Type("Bad bool".into())),
            },
            _ => Err(RelayError::Type("Bad bool".into())),
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
        _ => Err(RelayError::Type(format!("Unknown type hint: {ty}"))),
    }
}

// ========================= Binary ops =========================

fn eval_bin(op: BinOp, l: Value, r: Value) -> RResult<Value> {
    use BinOp::*;
    Ok(match op {
        And => Value::Bool(l.truthy() && r.truthy()),
        Or => Value::Bool(l.truthy() || r.truthy()),
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

fn assign_member_value(object: Value, name: &str, value: Value) -> RResult<Value> {
    match object {
        Value::Dict(mut map) => {
            map.insert(name.to_string(), value);
            Ok(Value::Dict(map))
        }
        Value::Json(mut j) => {
            let obj = j.as_object_mut().ok_or_else(|| {
                RelayError::Type("Member assignment on JSON expects object value".into())
            })?;
            obj.insert(name.to_string(), value_to_json(&value));
            Ok(Value::Json(j))
        }
        _ => Err(RelayError::Type(
            "Member assignment expects dict or JSON object".into(),
        )),
    }
}

fn assign_index_value(container: Value, index: Value, value: Value) -> RResult<Value> {
    match (container, index) {
        (Value::List(mut values), Value::Int(i)) => {
            let idx = usize::try_from(i)
                .map_err(|_| RelayError::Runtime("Index out of range".into()))?;
            let slot = values
                .get_mut(idx)
                .ok_or_else(|| RelayError::Runtime("Index out of range".into()))?;
            *slot = value;
            Ok(Value::List(values))
        }
        (Value::Dict(mut map), key) => {
            map.insert(key.repr(), value);
            Ok(Value::Dict(map))
        }
        (Value::Json(mut j), Value::Str(key)) => {
            let obj = j.as_object_mut().ok_or_else(|| {
                RelayError::Type("JSON indexing expects object value".into())
            })?;
            obj.insert(key, value_to_json(&value));
            Ok(Value::Json(j))
        }
        _ => Err(RelayError::Type(
            "Indexed assignment expects list/dict/json".into(),
        )),
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
                let out = match args.get(0).cloned().unwrap_or(Value::None) {
                    Value::Json(J::String(s)) => s,
                    other => other.repr(),
                };
                Ok(Value::Str(out))
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

    let evaluator_for_get_json = evaluator.clone();
    env.set(
        "get_json",
        Value::Builtin(Arc::new(move |_args, _| {
            let evaluator = evaluator_for_get_json.clone();
            Box::pin(async move {
                let request = {
                    let env = evaluator.env.lock().await;
                    env.get("request").ok()
                };

                match request {
                    Some(Value::Dict(req)) => Ok(req.get("json").cloned().unwrap_or(Value::None)),
                    _ => Ok(Value::None),
                }
            })
        })),
    );

    let evaluator_for_get_body = evaluator.clone();
    env.set(
        "get_body",
        Value::Builtin(Arc::new(move |_args, _| {
            let evaluator = evaluator_for_get_body.clone();
            Box::pin(async move {
                let request = {
                    let env = evaluator.env.lock().await;
                    env.get("request").ok()
                };

                match request {
                    Some(Value::Dict(req)) => {
                        Ok(req
                            .get("form")
                            .cloned()
                            .unwrap_or(Value::Dict(IndexMap::new())))
                    }
                    _ => Ok(Value::Dict(IndexMap::new())),
                }
            })
        })),
    );

    let evaluator_for_get_query = evaluator.clone();
    env.set(
        "get_query",
        Value::Builtin(Arc::new(move |_args, _| {
            let evaluator = evaluator_for_get_query.clone();
            Box::pin(async move {
                let request = {
                    let env = evaluator.env.lock().await;
                    env.get("request").ok()
                };

                match request {
                    Some(Value::Dict(req)) => {
                        Ok(req
                            .get("query")
                            .cloned()
                            .unwrap_or(Value::Dict(IndexMap::new())))
                    }
                    _ => Ok(Value::Dict(IndexMap::new())),
                }
            })
        })),
    );

    env.set(
        "validate",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let data = args.get(0).cloned().ok_or_else(|| {
                    RelayError::Type("validate(data, schema) missing data".into())
                })?;
                let schema = args.get(1).cloned().ok_or_else(|| {
                    RelayError::Type("validate(data, schema) missing schema".into())
                })?;
                validate_data_with_schema(data, schema, "validate(data, schema)")
            })
        })),
    );

    let evaluator_for_require_query = evaluator.clone();
    env.set(
        "require_query",
        Value::Builtin(Arc::new(move |args, _| {
            let evaluator = evaluator_for_require_query.clone();
            Box::pin(async move {
                let schema = args.get(0).cloned().ok_or_else(|| {
                    RelayError::Type("require_query(schema) missing schema".into())
                })?;
                let request = {
                    let env = evaluator.env.lock().await;
                    env.get("request").ok()
                };
                let data = match request {
                    Some(Value::Dict(req)) => req
                        .get("query")
                        .cloned()
                        .unwrap_or(Value::Dict(IndexMap::new())),
                    _ => Value::Dict(IndexMap::new()),
                };
                validate_data_with_schema(data, schema, "require_query(schema)")
            })
        })),
    );

    let evaluator_for_require_body = evaluator.clone();
    env.set(
        "require_body",
        Value::Builtin(Arc::new(move |args, _| {
            let evaluator = evaluator_for_require_body.clone();
            Box::pin(async move {
                let schema = args.get(0).cloned().ok_or_else(|| {
                    RelayError::Type("require_body(schema) missing schema".into())
                })?;
                let request = {
                    let env = evaluator.env.lock().await;
                    env.get("request").ok()
                };
                let data = match request {
                    Some(Value::Dict(req)) => req
                        .get("form")
                        .cloned()
                        .unwrap_or(Value::Dict(IndexMap::new())),
                    _ => Value::Dict(IndexMap::new()),
                };
                validate_data_with_schema(data, schema, "require_body(schema)")
            })
        })),
    );

    let evaluator_for_require_json = evaluator.clone();
    env.set(
        "require_json",
        Value::Builtin(Arc::new(move |args, _| {
            let evaluator = evaluator_for_require_json.clone();
            Box::pin(async move {
                let schema = args.get(0).cloned().ok_or_else(|| {
                    RelayError::Type("require_json(schema) missing schema".into())
                })?;
                let request = {
                    let env = evaluator.env.lock().await;
                    env.get("request").ok()
                };
                let data = match request {
                    Some(Value::Dict(req)) => match req.get("json").cloned() {
                        Some(v) => v,
                        None => {
                            return Err(RelayError::Type(
                                "require_json(schema) missing JSON body".into(),
                            ))
                        }
                    },
                    _ => {
                        return Err(RelayError::Type(
                            "require_json(schema) can only be used in a web handler".into(),
                        ))
                    }
                };
                validate_data_with_schema(data, schema, "require_json(schema)")
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

    let evaluator_for_http_error = evaluator.clone();
    env.set(
        "HTTPError",
        Value::Builtin(Arc::new(move |args, kwargs| {
            let evaluator = evaluator_for_http_error.clone();
            Box::pin(async move {
                let status = kwargs
                    .iter()
                    .find(|(k, _)| k == "status")
                    .map(|(_, v)| v.clone())
                    .or_else(|| args.get(0).cloned())
                    .unwrap_or(Value::Int(500));
                let code = kwargs
                    .iter()
                    .find(|(k, _)| k == "code")
                    .map(|(_, v)| v.clone())
                    .or_else(|| args.get(1).cloned())
                    .unwrap_or(Value::Str("internal_error".into()));
                let message = kwargs
                    .iter()
                    .find(|(k, _)| k == "message")
                    .map(|(_, v)| v.clone())
                    .or_else(|| args.get(2).cloned())
                    .unwrap_or(Value::Str("Internal server error".into()));
                let details = kwargs
                    .iter()
                    .find(|(k, _)| k == "details")
                    .map(|(_, v)| v.clone())
                    .or_else(|| args.get(3).cloned())
                    .unwrap_or(Value::None);

                let status = match status {
                    Value::Int(n) => n as u16,
                    Value::Float(f) => f as u16,
                    Value::Str(s) => s.parse::<u16>().unwrap_or(500),
                    _ => 500,
                };

                let details = match details {
                    Value::None => None,
                    other => Some(value_to_json(&other)),
                };

                let request_id = {
                    let env = evaluator.env.lock().await;
                    match env.get("request").ok() {
                        Some(Value::Dict(req)) => match req.get("request_id") {
                            Some(Value::Str(s)) => Some(s.clone()),
                            _ => None,
                        },
                        _ => None,
                    }
                };

                Ok(Value::Response(error_response(
                    status,
                    &code.repr(),
                    &message.repr(),
                    details,
                    request_id,
                )))
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
    env.set(
        "Email",
        Value::Builtin(Arc::new(|args, kwargs| {
            Box::pin(async move {
                let handle = EmailHandle::from_constructor_args(&args, &kwargs)?;
                Ok(Value::Obj(Object::Email(handle)))
            })
        })),
    );

    env.set(
        "auth_hash_password",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let password = expect_str(&args, 0, "auth_hash_password(password)")?;
                let salt = SaltString::generate(&mut OsRng);
                let hash = Argon2::default()
                    .hash_password(password.as_bytes(), &salt)
                    .map_err(|e| RelayError::Runtime(e.to_string()))?
                    .to_string();
                Ok(Value::Str(hash))
            })
        })),
    );

    env.set(
        "auth_verify_password",
        Value::Builtin(Arc::new(|args, _| {
            Box::pin(async move {
                let password = expect_str(&args, 0, "auth_verify_password(password, hash)")?;
                let hash = expect_str(&args, 1, "auth_verify_password(password, hash)")?;
                let parsed = match PasswordHash::new(&hash) {
                    Ok(h) => h,
                    Err(_) => return Ok(Value::Bool(false)),
                };
                Ok(Value::Bool(
                    Argon2::default()
                        .verify_password(password.as_bytes(), &parsed)
                        .is_ok(),
                ))
            })
        })),
    );

    let evaluator_for_auth_store = evaluator.clone();
    env.set(
        "AuthStore",
        Value::Builtin(Arc::new(move |args, _| {
            let evaluator = evaluator_for_auth_store.clone();
            Box::pin(async move {
                if args.is_empty() {
                    return Ok(Value::Obj(Object::AuthStore(AuthStoreHandle::memory())));
                }

                let load_fn = match args.get(0).cloned() {
                    Some(Value::Function(f)) => f.name.clone(),
                    _ => {
                        return Err(RelayError::Type(
                            "AuthStore(load_fn, save_fn) expects load_fn function".into(),
                        ))
                    }
                };
                let save_fn = match args.get(1).cloned() {
                    Some(Value::Function(f)) => f.name.clone(),
                    _ => {
                        return Err(RelayError::Type(
                            "AuthStore(load_fn, save_fn) expects save_fn function".into(),
                        ))
                    }
                };
                Ok(Value::Obj(Object::AuthStore(AuthStoreHandle::callback(
                    evaluator,
                    load_fn,
                    save_fn,
                ))))
            })
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

fn kwarg_or_arg(kwargs: &[(String, Value)], args: &[Value], name: &str, index: usize) -> Option<Value> {
    kwargs
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.clone())
        .or_else(|| args.get(index).cloned())
}

fn expect_str_value(v: Value, sig: &str, field: &str) -> RResult<String> {
    match v {
        Value::Str(s) => Ok(s),
        Value::Json(J::String(s)) => Ok(s),
        _ => Err(RelayError::Type(format!("{sig} {field} must be str"))),
    }
}

fn parse_optional_str_value(v: Option<Value>, sig: &str, field: &str) -> RResult<Option<String>> {
    match v {
        None | Some(Value::None) => Ok(None),
        Some(other) => Ok(Some(expect_str_value(other, sig, field)?)),
    }
}

fn parse_u16_value(v: Value, sig: &str, field: &str) -> RResult<u16> {
    let n = match v {
        Value::Int(i) => i,
        Value::Float(f) => f as i64,
        Value::Str(s) => s
            .trim()
            .parse::<i64>()
            .map_err(|_| RelayError::Type(format!("{sig} {field} must be int")))?,
        _ => return Err(RelayError::Type(format!("{sig} {field} must be int"))),
    };
    if !(0..=65535).contains(&n) {
        return Err(RelayError::Type(format!("{sig} {field} must be in range 0..65535")));
    }
    Ok(n as u16)
}

fn parse_usize_value(v: Value, sig: &str, field: &str) -> RResult<usize> {
    let n = match v {
        Value::Int(i) => i,
        Value::Float(f) => f as i64,
        Value::Str(s) => s
            .trim()
            .parse::<i64>()
            .map_err(|_| RelayError::Type(format!("{sig} {field} must be int")))?,
        _ => return Err(RelayError::Type(format!("{sig} {field} must be int"))),
    };
    if n <= 0 {
        return Err(RelayError::Type(format!(
            "{sig} {field} must be a positive integer"
        )));
    }
    Ok(n as usize)
}

fn parse_optional_mime_allowlist(
    v: Option<Value>,
    sig: &str,
    field: &str,
) -> RResult<Option<HashSet<String>>> {
    let Some(value) = v else {
        return Ok(None);
    };
    match value {
        Value::None => Ok(None),
        Value::Str(s) => {
            let mime = s.trim().to_ascii_lowercase();
            if mime.is_empty() {
                return Ok(None);
            }
            Ok(Some(HashSet::from([mime])))
        }
        Value::List(items) => {
            let mut set = HashSet::new();
            for item in items {
                let mime = expect_str_value(item, sig, field)?
                    .trim()
                    .to_ascii_lowercase();
                if !mime.is_empty() {
                    set.insert(mime);
                }
            }
            if set.is_empty() {
                Ok(None)
            } else {
                Ok(Some(set))
            }
        }
        Value::Json(J::Array(items)) => {
            parse_optional_mime_allowlist(Some(Value::List(items.iter().map(json_to_value).collect())), sig, field)
        }
        _ => Err(RelayError::Type(format!(
            "{sig} {field} must be str or list[str]"
        ))),
    }
}

fn parse_string_list_value(v: Value, sig: &str, field: &str) -> RResult<Vec<String>> {
    match v {
        Value::None => Ok(vec![]),
        Value::Str(s) => Ok(vec![s]),
        Value::Json(J::String(s)) => Ok(vec![s]),
        Value::List(items) => items
            .into_iter()
            .map(|item| expect_str_value(item, sig, field))
            .collect(),
        Value::Json(J::Array(items)) => items
            .iter()
            .map(|item| expect_str_value(json_to_value(item), sig, field))
            .collect(),
        _ => Err(RelayError::Type(format!("{sig} {field} must be str or list[str]"))),
    }
}

fn expect_object_like(v: Value, sig: &str) -> RResult<IndexMap<String, Value>> {
    match v {
        Value::Dict(m) => Ok(m),
        Value::Json(J::Object(obj)) => Ok(obj
            .iter()
            .map(|(k, vv)| (k.clone(), json_to_value(vv)))
            .collect()),
        _ => Err(RelayError::Type(format!("{sig} expects dict/json object"))),
    }
}

fn parse_schema_rule(
    field: &str,
    spec: &Value,
) -> RResult<(String, bool, Option<String>, Option<Value>)> {
    let mut required = true;
    let mut field_name = field.to_string();
    if let Some(stripped) = field.strip_suffix('?') {
        required = false;
        field_name = stripped.to_string();
    }

    match spec {
        Value::Str(ty) => Ok((field_name, required, Some(ty.clone()), None)),
        Value::Dict(rule) => {
            let mut expected_type = None;
            let mut default = None;
            if let Some(Value::Str(ty)) = rule.get("type") {
                expected_type = Some(ty.clone());
            }
            if let Some(v) = rule.get("required") {
                required = bool_from_value(v, "validate required flag")?;
            }
            if let Some(v) = rule.get("default") {
                default = Some(v.clone());
            }
            if expected_type.is_none() && default.is_none() {
                return Err(RelayError::Type(format!(
                    "validate(schema): rule for '{field_name}' needs 'type' or 'default'"
                )));
            }
            Ok((field_name, required, expected_type, default))
        }
        _ => Err(RelayError::Type(format!(
            "validate(schema): invalid schema for field '{field_name}'"
        ))),
    }
}

fn validate_data_with_schema(data: Value, schema: Value, sig: &str) -> RResult<Value> {
    let mut input = expect_object_like(data, sig)?;
    let schema_map = expect_object_like(schema, sig)?;

    for (raw_key, rule) in schema_map {
        let (field, required, expected_type, default) = parse_schema_rule(&raw_key, &rule)?;
        let existing = input.get(&field).cloned();

        match (existing, expected_type, default) {
            (Some(v), Some(expected_ty), _) => {
                let coerced = coerce_param_type(&expected_ty, v).map_err(|e| {
                    RelayError::Type(format!("validate({field}): {e}"))
                })?;
                input.insert(field, coerced);
            }
            (Some(_), None, Some(default)) => {
                input.insert(field, default);
            }
            (None, _, Some(default)) => {
                input.insert(field, default);
            }
            (None, Some(_), None) => {
                if required {
                    return Err(RelayError::Type(format!(
                        "validate({field}): missing required field"
                    )));
                }
            }
            (None, None, None) if required => {
                return Err(RelayError::Type(format!(
                    "validate({field}): missing required field"
                )))
            }
            (None, None, None) => {}
            (Some(_), None, None) => {}
        }
    }

    Ok(Value::Dict(input))
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

fn json_to_value(j: &J) -> Value {
    match j {
        J::Null => Value::None,
        J::Bool(b) => Value::Bool(*b),
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Str(n.to_string())
            }
        }
        J::String(s) => Value::Str(s.clone()),
        J::Array(items) => Value::List(items.iter().map(json_to_value).collect()),
        J::Object(obj) => {
            let mut out = IndexMap::new();
            for (k, v) in obj {
                out.insert(k.clone(), json_to_value(v));
            }
            Value::Dict(out)
        }
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

fn value_to_filter_document(v: Value, sig: &str) -> RResult<Document> {
    let mut doc = value_to_document(v, sig)?;
    for (k, v) in doc.iter_mut() {
        coerce_object_id_strings(v, k == "_id");
    }
    Ok(doc)
}

fn coerce_object_id_strings(value: &mut Bson, id_context: bool) {
    match value {
        Bson::String(s) if id_context => {
            if let Ok(oid) = ObjectId::parse_str(s.as_str()) {
                *value = Bson::ObjectId(oid);
            }
        }
        Bson::Document(doc) => {
            for (k, v) in doc.iter_mut() {
                coerce_object_id_strings(v, id_context || k == "_id");
            }
        }
        Bson::Array(items) => {
            for item in items.iter_mut() {
                coerce_object_id_strings(item, id_context);
            }
        }
        _ => {}
    }
}

fn bson_to_json(bson: Bson) -> RResult<J> {
    fn convert(bson: Bson) -> RResult<J> {
        match bson {
            Bson::ObjectId(oid) => Ok(J::String(oid.to_hex())),
            Bson::Document(doc) => {
                let mut out = serde_json::Map::new();
                for (k, v) in doc {
                    out.insert(k, convert(v)?);
                }
                Ok(J::Object(out))
            }
            Bson::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(convert(item)?);
                }
                Ok(J::Array(out))
            }
            other => serde_json::to_value(other).map_err(|e| RelayError::Runtime(e.to_string())),
        }
    }

    convert(bson)
}

// ========================= Web runtime (Axum) =========================

#[derive(Clone)]
struct WebAppHandle {
    inner: Arc<Mutex<AppState>>,
}
#[derive(Clone)]
struct RouteHandle {
    app: WebAppHandle,
    prefix: String,
}
#[derive(Clone)]
struct WebServerHandle;

struct AppState {
    routes: Vec<RouteSpec>,
    middlewares: Vec<String>,
    static_mounts: Vec<StaticMount>,
    sessions: HashMap<String, IndexMap<String, Value>>,
    session_config: SessionConfig,
    upload_config: UploadConfig,
    openapi: Option<OpenApiConfig>,
    session_backend: SessionBackendConfig,
}

#[derive(Clone)]
struct SessionConfig {
    secure: Option<bool>, // None => auto (https only)
    http_only: bool,
    same_site: String,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            secure: None,
            http_only: true,
            same_site: "Lax".to_string(),
        }
    }
}

#[derive(Clone)]
struct UploadConfig {
    max_body_bytes: usize,
    max_file_bytes: usize,
    allowed_mime_types: Option<HashSet<String>>,
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: 10 * 1024 * 1024, // 10 MiB
            max_file_bytes: 5 * 1024 * 1024,  // 5 MiB
            allowed_mime_types: None,
        }
    }
}

#[derive(Clone)]
struct OpenApiConfig {
    title: String,
    version: String,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            routes: Vec::new(),
            middlewares: Vec::new(),
            static_mounts: Vec::new(),
            sessions: HashMap::new(),
            session_config: SessionConfig::default(),
            upload_config: UploadConfig::default(),
            openapi: None,
            session_backend: SessionBackendConfig::Memory,
        }
    }
}

#[derive(Clone)]
enum SessionBackendConfig {
    Memory,
    Callback { load_fn: String, save_fn: String },
}
#[derive(Clone)]
struct RouteSpec {
    method: String,
    path: String,
    fn_name: String,
    validation: RouteValidation,
}

#[derive(Clone, Default)]
struct RouteValidation {
    validate_schema: Option<Value>,
    query_schema: Option<Value>,
    body_schema: Option<Value>,
    json_schema: Option<Value>,
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
        RouteHandle {
            app: self.clone(),
            prefix: String::new(),
        }
    }

    fn group(&self, prefix: String) -> RouteHandle {
        RouteHandle {
            app: self.clone(),
            prefix: normalize_group_prefix(&prefix),
        }
    }

    // New spec: decorator attaches directly to the app: @app.get("/"), @app.post("/")
    fn register(
        &self,
        method: String,
        path: String,
        fn_name: String,
        validation: RouteValidation,
    ) {
        let mut st = self.inner.lock().unwrap();
        st.routes.push(RouteSpec {
            method,
            path,
            fn_name,
            validation,
        });
    }

    fn use_middleware(&self, fn_name: String) {
        let mut st = self.inner.lock().unwrap();
        st.middlewares.push(fn_name);
    }

    fn set_session_config(&self, cfg: SessionConfig) {
        let mut st = self.inner.lock().unwrap();
        st.session_config = cfg;
    }

    fn session_config(&self) -> SessionConfig {
        let st = self.inner.lock().unwrap();
        st.session_config.clone()
    }

    fn set_upload_config(&self, cfg: UploadConfig) {
        let mut st = self.inner.lock().unwrap();
        st.upload_config = cfg;
    }

    fn upload_config(&self) -> UploadConfig {
        let st = self.inner.lock().unwrap();
        st.upload_config.clone()
    }

    fn set_openapi(&self, cfg: OpenApiConfig) {
        let mut st = self.inner.lock().unwrap();
        st.openapi = Some(cfg);
    }

    fn openapi_config(&self) -> Option<OpenApiConfig> {
        let st = self.inner.lock().unwrap();
        st.openapi.clone()
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

    fn set_session_backend(&self, backend: SessionBackendConfig) {
        let mut st = self.inner.lock().unwrap();
        st.session_backend = backend;
    }

    fn session_backend(&self) -> SessionBackendConfig {
        let st = self.inner.lock().unwrap();
        st.session_backend.clone()
    }
}
impl RouteHandle {
    fn group(&self, prefix: String) -> RouteHandle {
        let prefix = join_route_prefix(&self.prefix, &prefix);
        RouteHandle {
            app: self.app.clone(),
            prefix,
        }
    }

    fn register(
        &self,
        method: String,
        path: String,
        fn_name: String,
        validation: RouteValidation,
    ) {
        let full_path = join_route_prefix(&self.prefix, &path);
        let mut st = self.app.inner.lock().unwrap();
        st.routes.push(RouteSpec {
            method,
            path: full_path,
            fn_name,
            validation,
        });
    }
}

// Callback from Axum -> interpreter
#[derive(Clone)]
struct RequestParts {
    method: String,
    path: String,
    request_id: String,
    route_validation: RouteValidation,
    path_params: HashMap<String, String>,
    query: HashMap<String, String>,
    json: Option<J>,
    form: HashMap<String, Value>,
    headers: HashMap<String, String>,
    cookies: HashMap<String, String>,
    session_id: Option<String>,
    websocket: Option<WebSocketHandle>,
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
    let (specs, static_mounts, openapi_cfg) = {
        let state = app.inner.lock().unwrap();
        (
            state.routes.clone(),
            state.static_mounts.clone(),
            state.openapi.clone(),
        )
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
                Ok::<_, (StatusCode, String)>(
                    value_to_axum_response(Value::Response(response))
                        .await
                        .into_response(),
                )
            }
        };

        let dir_for_files = dir.clone();
        let route_for_files = route.clone();
        let static_handler = move |Path(file): Path<String>| {
            let dir = dir_for_files.clone();
            let route_prefix = route_for_files.clone();
            async move {
                let response = serve_static_file(&route_prefix, &dir, &file).await;
                Ok::<_, (StatusCode, String)>(
                    value_to_axum_response(Value::Response(response))
                        .await
                        .into_response(),
                )
            }
        };

        router = router
            .route(&route, get(index_handler))
            .route(&path_pattern, get(static_handler));
    }

    if let Some(cfg) = openapi_cfg {
        let doc = build_openapi_doc(&specs, &cfg.title, &cfg.version);
        router = router.route(
            "/openapi.json",
            get(move || {
                let doc = doc.clone();
                async move {
                    let response = Response {
                        status: 200,
                        content_type: "application/json".into(),
                        body: doc.to_string().into_bytes(),
                        headers: HashMap::new(),
                        set_cookies: vec![],
                    };
                    Ok::<_, (StatusCode, String)>(
                        value_to_axum_response(Value::Response(response))
                            .await
                            .into_response(),
                    )
                }
            }),
        );
    }

    for r in specs {
        let method = r.method.to_lowercase();
        let path = relay_path_to_axum(&r.path);
        let fn_name = r.fn_name.clone();
        let route_path = r.path.clone();
        let route_validation = r.validation.clone();
        let app_handle = app.clone();

        let http_fn_name = fn_name.clone();
        let http_route_path = route_path.clone();
        let http_route_validation = route_validation.clone();
        let http_app_handle = app_handle.clone();
        let ws_fn_name = fn_name.clone();
        let ws_route_path = route_path.clone();
        let ws_route_validation = route_validation.clone();
        let ws_app_handle = app_handle.clone();

        // define handlers in THIS scope so route registration can capture them
        let handler = move |method: Method,
                            Path(ax_path): Path<HashMap<String, String>>,
                            Query(q): Query<HashMap<String, String>>,
                            headers: HeaderMap,
                            body: Bytes| {
            let fn_name = http_fn_name.clone();
            let route_path = http_route_path.clone();
            let route_validation = http_route_validation.clone();
            let app_handle = http_app_handle.clone();
            async move {
                let mut h = HashMap::new();
                for (k, v) in headers.iter() {
                    if let Ok(s) = v.to_str() {
                        h.insert(k.to_string(), s.to_string());
                    }
                }
                let cookies = parse_cookie_header(h.get("cookie").cloned());
                let session_id = cookies.get("relay_sid").cloned();
                let request_id = h
                    .get("x-request-id")
                    .cloned()
                    .filter(|v| !v.trim().is_empty())
                    .unwrap_or_else(new_request_id);
                let upload_config = app_handle.upload_config();
                let (json_body, form_body) =
                    match parse_request_body(&headers, body, &upload_config).await {
                    Ok(parts) => parts,
                    Err(e) => {
                        let error = error_response(
                            400,
                            "bad_request",
                            &e.to_string(),
                            None,
                            Some(request_id),
                        );
                        return Ok::<_, (StatusCode, String)>(
                            value_to_axum_response(Value::Response(error))
                                .await
                                .into_response(),
                        );
                    }
                };

                let req = RequestParts {
                    method: method.to_string(),
                    path: route_path,
                    request_id: request_id.clone(),
                    route_validation,
                    path_params: ax_path,
                    query: q,
                    json: json_body,
                    form: form_body,
                    headers: h,
                    cookies,
                    session_id,
                    websocket: None,
                };

                let cb = get_global_callback().ok_or_else(|| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "No relay callback".to_string(),
                    )
                })?;

                let v = match cb(app_handle, fn_name, req).await {
                    Ok(v) => v,
                    Err(e) => Value::Response(error_response(
                        500,
                        "internal_error",
                        &e.to_string(),
                        None,
                        Some(request_id),
                    )),
                };

                Ok::<_, (StatusCode, String)>(value_to_axum_response(v).await.into_response())
            }
        };

        let ws_handler = move |ws: WebSocketUpgrade,
                               Path(ax_path): Path<HashMap<String, String>>,
                               Query(q): Query<HashMap<String, String>>,
                               headers: HeaderMap| {
            let fn_name = ws_fn_name.clone();
            let route_path = ws_route_path.clone();
            let route_validation = ws_route_validation.clone();
            let app_handle = ws_app_handle.clone();
            async move {
                let mut h = HashMap::new();
                for (k, v) in headers.iter() {
                    if let Ok(s) = v.to_str() {
                        h.insert(k.to_string(), s.to_string());
                    }
                }
                let cookies = parse_cookie_header(h.get("cookie").cloned());
                let session_id = cookies.get("relay_sid").cloned();
                let request_id = h
                    .get("x-request-id")
                    .cloned()
                    .filter(|v| !v.trim().is_empty())
                    .unwrap_or_else(new_request_id);

                let response = ws.on_upgrade(move |socket| async move {
                    let cb = match get_global_callback() {
                        Some(cb) => cb,
                        None => {
                            eprintln!("WebSocket route handler callback is not set");
                            return;
                        }
                    };

                    let req = RequestParts {
                        method: "WS".to_string(),
                        path: route_path,
                        request_id,
                        route_validation,
                        path_params: ax_path,
                        query: q,
                        json: None,
                        form: HashMap::new(),
                        headers: h,
                        cookies,
                        session_id,
                        websocket: Some(WebSocketHandle {
                            inner: Arc::new(tokio::sync::Mutex::new(socket)),
                        }),
                    };

                    if let Err(e) = cb(app_handle, fn_name, req).await {
                        eprintln!("WebSocket handler error: {e}");
                    }
                });

                Ok::<_, (StatusCode, String)>(response.into_response())
            }
        };

        router = match method.as_str() {
            "get" => router.route(&path, get(handler)),
            "post" => router.route(&path, post(handler)),
            "put" => router.route(&path, axum::routing::put(handler)),
            "patch" => router.route(&path, axum::routing::patch(handler)),
            "delete" => router.route(&path, axum::routing::delete(handler)),
            "ws" => router.route(&path, get(ws_handler)),
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

fn push_form_value(form: &mut HashMap<String, Value>, key: String, value: Value) {
    use std::collections::hash_map::Entry;
    match form.entry(key) {
        Entry::Vacant(slot) => {
            slot.insert(value);
        }
        Entry::Occupied(mut occ) => match occ.get_mut() {
            Value::List(items) => items.push(value),
            existing => {
                let previous = existing.clone();
                *existing = Value::List(vec![previous, value]);
            }
        },
    }
}

fn build_uploaded_file_value(
    filename: Option<String>,
    content_type: Option<String>,
    bytes: Vec<u8>,
) -> Value {
    let mut file = IndexMap::new();
    file.insert(
        "filename".into(),
        filename.map(Value::Str).unwrap_or(Value::None),
    );
    file.insert(
        "content_type".into(),
        content_type.map(Value::Str).unwrap_or(Value::None),
    );
    file.insert("size".into(), Value::Int(bytes.len() as i64));
    file.insert("bytes".into(), Value::Bytes(bytes));
    Value::Dict(file)
}

async fn parse_multipart_form_data(
    content_type: &str,
    body: Bytes,
    upload_config: &UploadConfig,
) -> RResult<HashMap<String, Value>> {
    let boundary = multer::parse_boundary(content_type)
        .map_err(|e| RelayError::Type(format!("multipart/form-data boundary parse failed: {e}")))?;

    let stream = futures::stream::once(async move { Ok::<Bytes, std::io::Error>(body) });
    let mut multipart = multer::Multipart::new(stream, boundary);
    let mut form = HashMap::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| RelayError::Type(format!("multipart/form-data parse failed: {e}")))?
    {
        let Some(name) = field.name().map(|s| s.to_string()) else {
            continue;
        };
        let filename = field.file_name().map(|s| s.to_string());
        let content_type = field.content_type().map(|s| s.to_string());
        let bytes = field
            .bytes()
            .await
            .map_err(|e| RelayError::Type(format!("multipart field read failed: {e}")))?;

        let value = if filename.is_some() {
            if bytes.len() > upload_config.max_file_bytes {
                return Err(RelayError::Type(format!(
                    "multipart file field '{name}' exceeds max_file_bytes={} (got {} bytes)",
                    upload_config.max_file_bytes,
                    bytes.len()
                )));
            }

            if let Some(allowed) = &upload_config.allowed_mime_types {
                let normalized = content_type
                    .as_deref()
                    .map(|v| v.trim().to_ascii_lowercase())
                    .unwrap_or_default();
                if normalized.is_empty() || !allowed.contains(&normalized) {
                    return Err(RelayError::Type(format!(
                        "multipart file field '{name}' content type '{}' is not allowed",
                        if normalized.is_empty() {
                            "<missing>"
                        } else {
                            normalized.as_str()
                        }
                    )));
                }
            }

            build_uploaded_file_value(filename, content_type, bytes.to_vec())
        } else {
            Value::Str(String::from_utf8_lossy(&bytes).to_string())
        };
        push_form_value(&mut form, name, value);
    }

    Ok(form)
}

async fn parse_request_body(
    headers: &HeaderMap,
    body: Bytes,
    upload_config: &UploadConfig,
) -> RResult<(Option<J>, HashMap<String, Value>)> {
    if body.is_empty() {
        return Ok((None, HashMap::new()));
    }

    if body.len() > upload_config.max_body_bytes {
        return Err(RelayError::Type(format!(
            "request body exceeds max_body_bytes={} (got {} bytes)",
            upload_config.max_body_bytes,
            body.len()
        )));
    }

    let ct_raw = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let ct = ct_raw.to_ascii_lowercase();

    if ct.contains("application/json") {
        if let Ok(json) = serde_json::from_slice::<J>(&body) {
            return Ok((Some(json), HashMap::new()));
        }
        return Ok((None, HashMap::new()));
    }

    if ct.contains("application/x-www-form-urlencoded") {
        let mut form = HashMap::new();
        for (k, v) in url::form_urlencoded::parse(&body).into_owned() {
            push_form_value(&mut form, k, Value::Str(v));
        }
        return Ok((None, form));
    }

    if ct.contains("multipart/form-data") {
        let form = parse_multipart_form_data(&ct_raw, body, upload_config).await?;
        return Ok((None, form));
    }

    Ok((None, HashMap::new()))
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

fn normalize_group_prefix(prefix: &str) -> String {
    if prefix.trim().is_empty() || prefix == "/" {
        return String::new();
    }
    let mut p = prefix.trim().to_string();
    if !p.starts_with('/') {
        p.insert(0, '/');
    }
    p.trim_end_matches('/').to_string()
}

fn join_route_prefix(prefix: &str, path: &str) -> String {
    let pfx = normalize_group_prefix(prefix);
    let leaf = if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    if pfx.is_empty() {
        leaf
    } else if leaf == "/" {
        pfx
    } else {
        format!("{pfx}{leaf}")
    }
}

fn new_session_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("rsid_{now:x}")
}

fn new_request_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("rid_{now:x}")
}

fn request_is_https(headers: &HashMap<String, String>) -> bool {
    headers
        .get("x-forwarded-proto")
        .map(|v| v.eq_ignore_ascii_case("https"))
        .unwrap_or(false)
}

fn bool_from_value(v: &Value, sig: &str) -> RResult<bool> {
    match v {
        Value::Bool(b) => Ok(*b),
        Value::Int(i) => Ok(*i != 0),
        Value::Str(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            _ => Err(RelayError::Type(format!("{sig} expects bool"))),
        },
        _ => Err(RelayError::Type(format!("{sig} expects bool"))),
    }
}

fn build_session_cookie(
    sid: &str,
    session_cfg: &SessionConfig,
    request_headers: &HashMap<String, String>,
) -> String {
    let mut attrs = vec![format!("relay_sid={sid}"), "Path=/".to_string()];
    if session_cfg.http_only {
        attrs.push("HttpOnly".to_string());
    }
    attrs.push(format!("SameSite={}", session_cfg.same_site));
    let secure = session_cfg
        .secure
        .unwrap_or_else(|| request_is_https(request_headers));
    if secure {
        attrs.push("Secure".to_string());
    }
    attrs.join("; ")
}

fn error_response(
    status: u16,
    code: &str,
    message: &str,
    details: Option<J>,
    request_id: Option<String>,
) -> Response {
    let mut error_obj = serde_json::Map::new();
    error_obj.insert("code".into(), J::String(code.to_string()));
    error_obj.insert("message".into(), J::String(message.to_string()));
    if let Some(d) = details {
        error_obj.insert("details".into(), d);
    }
    if let Some(rid) = request_id.clone() {
        error_obj.insert("request_id".into(), J::String(rid));
    }

    let mut root = serde_json::Map::new();
    root.insert("error".into(), J::Object(error_obj));

    let mut headers = HashMap::new();
    if let Some(rid) = request_id {
        headers.insert("x-request-id".into(), rid);
    }

    Response {
        status,
        content_type: "application/json".into(),
        body: J::Object(root).to_string().into_bytes(),
        headers,
        set_cookies: vec![],
    }
}

fn status_to_error_code(status: u16) -> &'static str {
    match status {
        400 => "bad_request",
        401 => "unauthorized",
        403 => "forbidden",
        404 => "not_found",
        405 => "method_not_allowed",
        409 => "conflict",
        422 => "validation_error",
        429 => "rate_limited",
        500 => "internal_error",
        502 => "bad_gateway",
        503 => "service_unavailable",
        504 => "gateway_timeout",
        _ if (400..500).contains(&status) => "client_error",
        _ => "server_error",
    }
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

fn relay_path_to_openapi(p: &str) -> String {
    let mut out = String::new();
    let bytes = p.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] as char == '<' {
            if let Some(end) = p[i + 1..].find('>') {
                let name = &p[i + 1..i + 1 + end];
                out.push('{');
                out.push_str(name);
                out.push('}');
                i += end + 2;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn build_openapi_doc(specs: &[RouteSpec], title: &str, version: &str) -> J {
    let mut paths = serde_json::Map::new();

    for r in specs {
        if r.method.eq_ignore_ascii_case("ws") {
            continue;
        }
        let path = relay_path_to_openapi(&r.path);
        let method = r.method.to_ascii_lowercase();
        let mut op = serde_json::Map::new();
        op.insert("operationId".into(), J::String(r.fn_name.clone()));
        op.insert(
            "responses".into(),
            serde_json::json!({
                "200": { "description": "Success" },
                "400": { "description": "Bad Request" },
                "500": { "description": "Internal Server Error" }
            }),
        );
        let path_entry = paths
            .entry(path)
            .or_insert_with(|| J::Object(serde_json::Map::new()));
        if let J::Object(method_map) = path_entry {
            method_map.insert(method, J::Object(op));
        }
    }

    serde_json::json!({
        "openapi": "3.0.0",
        "info": {
            "title": title,
            "version": version
        },
        "paths": J::Object(paths)
    })
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

fn parse_headers_value(v: Value, sig: &str) -> RResult<HashMap<String, String>> {
    match v {
        Value::None => Ok(HashMap::new()),
        Value::Dict(m) => {
            let mut out = HashMap::new();
            for (k, vv) in m {
                out.insert(k, vv.repr());
            }
            Ok(out)
        }
        Value::Json(J::Object(obj)) => {
            let mut out = HashMap::new();
            for (k, vv) in obj {
                out.insert(k, vv.to_string().trim_matches('"').to_string());
            }
            Ok(out)
        }
        _ => Err(RelayError::Type(format!("{sig} headers must be dict/json object"))),
    }
}

fn apply_http_body(req: reqwest::RequestBuilder, data: Value) -> reqwest::RequestBuilder {
    match data {
        Value::None => req,
        Value::Json(j) => req.json(&j),
        Value::Dict(m) => {
            let mut obj = serde_json::Map::new();
            for (k, vv) in m {
                obj.insert(k, value_to_json(&vv));
            }
            req.json(&J::Object(obj))
        }
        Value::Str(s) => req.body(s),
        Value::Bytes(b) => req.body(b),
        other => req.body(other.repr()),
    }
}

async fn execute_http_request(
    client: reqwest::Client,
    method: &str,
    url: String,
    data: Value,
    headers: HashMap<String, String>,
) -> RResult<Value> {
    let mut req = match method {
        "GET" => client.get(url),
        "POST" => client.post(url),
        "PUT" => client.put(url),
        "PATCH" => client.patch(url),
        "DELETE" => client.delete(url),
        _ => return Err(RelayError::Runtime(format!("Unsupported HTTP method: {method}"))),
    };

    for (k, v) in headers {
        req = req.header(&k, &v);
    }
    req = apply_http_body(req, data);

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
            "get" => Value::Builtin(Arc::new(move |args, kwargs| {
                let client = client.clone();
                Box::pin(async move {
                    let url = expect_str(&args, 0, "http.get(url)")?;
                    let headers = kwargs
                        .iter()
                        .find(|(k, _)| k == "headers")
                        .map(|(_, v)| v.clone())
                        .or_else(|| args.get(1).cloned())
                        .map(|v| parse_headers_value(v, "http.get(url, headers=...)"))
                        .transpose()?
                        .unwrap_or_default();
                    let d = Deferred::new(Box::pin(async move {
                        execute_http_request(client, "GET", url, Value::None, headers).await
                    }));
                    Ok(Value::Deferred(Arc::new(d)))
                })
            })),
            "post" => Value::Builtin(Arc::new(move |args, kwargs| {
                let client = client.clone();
                Box::pin(async move {
                    let url = expect_str(&args, 0, "http.post(url, data)")?;
                    let data = kwargs
                        .iter()
                        .find(|(k, _)| k == "data")
                        .map(|(_, v)| v.clone())
                        .or_else(|| args.get(1).cloned())
                        .unwrap_or(Value::None);
                    let headers = kwargs
                        .iter()
                        .find(|(k, _)| k == "headers")
                        .map(|(_, v)| v.clone())
                        .or_else(|| args.get(2).cloned())
                        .map(|v| parse_headers_value(v, "http.post(url, data, headers=...)"))
                        .transpose()?
                        .unwrap_or_default();
                    let d = Deferred::new(Box::pin(async move {
                        execute_http_request(client, "POST", url, data, headers).await
                    }));
                    Ok(Value::Deferred(Arc::new(d)))
                })
            })),
            "put" => Value::Builtin(Arc::new(move |args, kwargs| {
                let client = client.clone();
                Box::pin(async move {
                    let url = expect_str(&args, 0, "http.put(url, data)")?;
                    let data = kwargs
                        .iter()
                        .find(|(k, _)| k == "data")
                        .map(|(_, v)| v.clone())
                        .or_else(|| args.get(1).cloned())
                        .unwrap_or(Value::None);
                    let headers = kwargs
                        .iter()
                        .find(|(k, _)| k == "headers")
                        .map(|(_, v)| v.clone())
                        .or_else(|| args.get(2).cloned())
                        .map(|v| parse_headers_value(v, "http.put(url, data, headers=...)"))
                        .transpose()?
                        .unwrap_or_default();
                    let d = Deferred::new(Box::pin(async move {
                        execute_http_request(client, "PUT", url, data, headers).await
                    }));
                    Ok(Value::Deferred(Arc::new(d)))
                })
            })),
            "patch" => Value::Builtin(Arc::new(move |args, kwargs| {
                let client = client.clone();
                Box::pin(async move {
                    let url = expect_str(&args, 0, "http.patch(url, data)")?;
                    let data = kwargs
                        .iter()
                        .find(|(k, _)| k == "data")
                        .map(|(_, v)| v.clone())
                        .or_else(|| args.get(1).cloned())
                        .unwrap_or(Value::None);
                    let headers = kwargs
                        .iter()
                        .find(|(k, _)| k == "headers")
                        .map(|(_, v)| v.clone())
                        .or_else(|| args.get(2).cloned())
                        .map(|v| parse_headers_value(v, "http.patch(url, data, headers=...)"))
                        .transpose()?
                        .unwrap_or_default();
                    let d = Deferred::new(Box::pin(async move {
                        execute_http_request(client, "PATCH", url, data, headers).await
                    }));
                    Ok(Value::Deferred(Arc::new(d)))
                })
            })),
            "delete" => Value::Builtin(Arc::new(move |args, kwargs| {
                let client = client.clone();
                Box::pin(async move {
                    let url = expect_str(&args, 0, "http.delete(url)")?;
                    let data = kwargs
                        .iter()
                        .find(|(k, _)| k == "data")
                        .map(|(_, v)| v.clone())
                        .or_else(|| args.get(1).cloned())
                        .unwrap_or(Value::None);
                    let headers = kwargs
                        .iter()
                        .find(|(k, _)| k == "headers")
                        .map(|(_, v)| v.clone())
                        .or_else(|| args.get(2).cloned())
                        .map(|v| parse_headers_value(v, "http.delete(url, headers=...)"))
                        .transpose()?
                        .unwrap_or_default();
                    let d = Deferred::new(Box::pin(async move {
                        execute_http_request(client, "DELETE", url, data, headers).await
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
            "headers" => Value::Dict(
                self.headers
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
                    .collect(),
            ),
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

// ========================= Email Client (SMTP) =========================

type EmailSendFut = Pin<Box<dyn Future<Output = RResult<EmailSendMetadata>> + Send>>;
type EmailSender = Arc<dyn Fn(MailMessage) -> EmailSendFut + Send + Sync>;

#[derive(Clone, Copy)]
enum EmailTlsMode {
    StartTls,
    Wrapper,
    Insecure,
}

impl EmailTlsMode {
    fn parse(raw: &str) -> RResult<Self> {
        match raw.trim().to_lowercase().as_str() {
            "" | "starttls" => Ok(Self::StartTls),
            "wrapper" | "smtps" => Ok(Self::Wrapper),
            "insecure" => Ok(Self::Insecure),
            other => Err(RelayError::Type(format!(
                "Email(..., tls=...) expects one of: starttls, wrapper, insecure (got '{other}')"
            ))),
        }
    }
}

#[derive(Clone)]
struct EmailConfig {
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
    default_from: Option<String>,
    tls_mode: EmailTlsMode,
}

#[derive(Clone, Default)]
struct EmailSendMetadata {
    message_id: Option<String>,
    smtp_code: Option<i64>,
    smtp_message: Option<String>,
}

struct PreparedEmail {
    message: MailMessage,
    envelope_to: Vec<String>,
    message_id: Option<String>,
}

#[derive(Clone)]
struct EmailAttachmentSpec {
    filename: String,
    content_type: String,
    bytes: Vec<u8>,
}

#[derive(Clone)]
struct EmailHandle {
    config: EmailConfig,
    sender: EmailSender,
}

fn parse_mailbox(raw: String, sig: &str, field: &str) -> RResult<Mailbox> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(RelayError::Type(format!("{sig} {field} cannot be empty")));
    }
    trimmed.parse::<Mailbox>().map_err(|e| {
        RelayError::Type(format!(
            "{sig} {field} has invalid mailbox '{trimmed}': {e}"
        ))
    })
}

fn parse_optional_mailbox(v: Option<Value>, sig: &str, field: &str) -> RResult<Option<Mailbox>> {
    let Some(raw) = parse_optional_str_value(v, sig, field)? else {
        return Ok(None);
    };
    Ok(Some(parse_mailbox(raw, sig, field)?))
}

fn parse_mailbox_list(v: Value, sig: &str, field: &str) -> RResult<Vec<Mailbox>> {
    parse_string_list_value(v, sig, field)?
        .into_iter()
        .map(|raw| parse_mailbox(raw, sig, field))
        .collect()
}

fn parse_template_locals(v: Option<Value>, sig: &str) -> RResult<HashMap<String, Value>> {
    match v.unwrap_or(Value::None) {
        Value::None => Ok(HashMap::new()),
        Value::Dict(m) => Ok(m.into_iter().collect()),
        Value::Json(J::Object(obj)) => Ok(obj
            .iter()
            .map(|(k, vv)| (k.clone(), json_to_value(vv)))
            .collect()),
        _ => Err(RelayError::Type(format!(
            "{sig} data must be dict/json object"
        ))),
    }
}

fn parse_attachment_bytes(v: Value, sig: &str) -> RResult<Vec<u8>> {
    match v {
        Value::Bytes(b) => Ok(b),
        Value::Str(s) => Ok(s.into_bytes()),
        Value::Json(J::String(s)) => Ok(s.into_bytes()),
        Value::List(values) => {
            let mut out = Vec::with_capacity(values.len());
            for item in values {
                let n = match item {
                    Value::Int(i) => i,
                    Value::Float(f) => f as i64,
                    _ => {
                        return Err(RelayError::Type(format!(
                            "{sig} attachments bytes list must contain ints"
                        )))
                    }
                };
                if !(0..=255).contains(&n) {
                    return Err(RelayError::Type(format!(
                        "{sig} attachments bytes list values must be in range 0..255"
                    )));
                }
                out.push(n as u8);
            }
            Ok(out)
        }
        Value::Json(J::Array(values)) => parse_attachment_bytes(Value::List(
            values.iter().map(json_to_value).collect(),
        ), sig),
        _ => Err(RelayError::Type(format!(
            "{sig} attachment content must be bytes, str, or list[int]"
        ))),
    }
}

fn parse_email_attachment(v: Value, sig: &str) -> RResult<EmailAttachmentSpec> {
    let mut map = expect_object_like(v, sig)?;
    let filename = map
        .remove("filename")
        .ok_or_else(|| RelayError::Type(format!("{sig} attachment missing filename")))?;
    let filename = expect_str_value(filename, sig, "attachment.filename")?;

    let content_type = parse_optional_str_value(
        map.remove("content_type"),
        sig,
        "attachment.content_type",
    )?
    .unwrap_or_else(|| "application/octet-stream".to_string());

    let body_value = map
        .remove("bytes")
        .or_else(|| map.remove("content"))
        .or_else(|| map.remove("data"))
        .ok_or_else(|| {
            RelayError::Type(format!(
                "{sig} attachment requires bytes/content/data field"
            ))
        })?;
    let bytes = parse_attachment_bytes(body_value, sig)?;

    Ok(EmailAttachmentSpec {
        filename,
        content_type,
        bytes,
    })
}

fn parse_email_attachments(v: Option<Value>, sig: &str) -> RResult<Vec<EmailAttachmentSpec>> {
    let Some(v) = v else {
        return Ok(vec![]);
    };
    match v {
        Value::None => Ok(vec![]),
        Value::Dict(_) | Value::Json(J::Object(_)) => Ok(vec![parse_email_attachment(v, sig)?]),
        Value::List(items) => items
            .into_iter()
            .map(|item| parse_email_attachment(item, sig))
            .collect(),
        Value::Json(J::Array(items)) => items
            .iter()
            .map(|item| parse_email_attachment(json_to_value(item), sig))
            .collect(),
        _ => Err(RelayError::Type(format!(
            "{sig} attachments must be attachment dict or list[attachment]"
        ))),
    }
}

fn smtp_sender_for_config(config: &EmailConfig) -> EmailSender {
    let host = config.host.clone();
    let port = config.port;
    let username = config.username.clone();
    let password = config.password.clone();
    let tls_mode = config.tls_mode;

    Arc::new(move |message: MailMessage| {
        let host = host.clone();
        let username = username.clone();
        let password = password.clone();
        Box::pin(async move {
            let mut builder = match tls_mode {
                EmailTlsMode::StartTls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(
                    host.as_str(),
                )
                .map_err(|e| RelayError::Runtime(format!("email transport setup failed: {e}")))?,
                EmailTlsMode::Wrapper => AsyncSmtpTransport::<Tokio1Executor>::relay(
                    host.as_str(),
                )
                .map_err(|e| RelayError::Runtime(format!("email transport setup failed: {e}")))?,
                EmailTlsMode::Insecure => {
                    AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(host.as_str())
                }
            };
            builder = builder.port(port);

            if let (Some(user), Some(pass)) = (username, password) {
                builder = builder.credentials(Credentials::new(user, pass));
            }

            let transport = builder.build();
            let response = transport
                .send(message)
                .await
                .map_err(|e| RelayError::Runtime(format!("email send failed: {e}")))?;

            let smtp_code = response.code().to_string().parse::<i64>().ok();
            let smtp_message_lines = response
                .message()
                .map(|line| line.to_string())
                .collect::<Vec<_>>();
            let smtp_message = if smtp_message_lines.is_empty() {
                None
            } else {
                Some(smtp_message_lines.join("\n"))
            };

            Ok(EmailSendMetadata {
                message_id: None,
                smtp_code,
                smtp_message,
            })
        })
    })
}

impl EmailHandle {
    fn new(config: EmailConfig) -> Self {
        let sender = smtp_sender_for_config(&config);
        Self { config, sender }
    }

    fn from_constructor_args(args: &[Value], kwargs: &[(String, Value)]) -> RResult<Self> {
        let sig = "Email(host, port=587, username=None, password=None, from=None, tls=\"starttls\")";

        let host = kwarg_or_arg(kwargs, args, "host", 0).ok_or_else(|| {
            RelayError::Type(format!("{sig} missing host"))
        })?;
        let host = expect_str_value(host, sig, "host")?;

        let port = kwarg_or_arg(kwargs, args, "port", 1).unwrap_or(Value::Int(587));
        let port = parse_u16_value(port, sig, "port")?;

        let username = parse_optional_str_value(kwarg_or_arg(kwargs, args, "username", 2), sig, "username")?;
        let password = parse_optional_str_value(kwarg_or_arg(kwargs, args, "password", 3), sig, "password")?;
        let default_from = parse_optional_str_value(kwarg_or_arg(kwargs, args, "from", 4), sig, "from")?;

        let tls_raw = parse_optional_str_value(kwarg_or_arg(kwargs, args, "tls", 5), sig, "tls")?
            .unwrap_or_else(|| "starttls".to_string());
        let tls_mode = EmailTlsMode::parse(&tls_raw)?;

        if username.is_some() ^ password.is_some() {
            return Err(RelayError::Type(format!(
                "{sig} username and password must both be provided or both omitted"
            )));
        }

        Ok(Self::new(EmailConfig {
            host,
            port,
            username,
            password,
            default_from,
            tls_mode,
        }))
    }

    #[cfg(test)]
    fn with_sender_for_test(config: EmailConfig, sender: EmailSender) -> Self {
        Self { config, sender }
    }

    fn prepare_send_request(&self, args: &[Value], kwargs: &[(String, Value)]) -> RResult<PreparedEmail> {
        let sig = "email.send(to, subject, text=None, html=None, cc=None, bcc=None, reply_to=None, from=None, headers=None, attachments=None)";

        let to_raw = kwarg_or_arg(kwargs, args, "to", 0)
            .ok_or_else(|| RelayError::Type(format!("{sig} missing to")))?;
        let to = parse_mailbox_list(to_raw, sig, "to")?;
        if to.is_empty() {
            return Err(RelayError::Type(format!("{sig} to cannot be empty")));
        }

        let subject = kwarg_or_arg(kwargs, args, "subject", 1)
            .ok_or_else(|| RelayError::Type(format!("{sig} missing subject")))?;
        let subject = expect_str_value(subject, sig, "subject")?;

        let text = parse_optional_str_value(kwarg_or_arg(kwargs, args, "text", 2), sig, "text")?;
        let html = parse_optional_str_value(kwarg_or_arg(kwargs, args, "html", 3), sig, "html")?;
        if text.is_none() && html.is_none() {
            return Err(RelayError::Type(format!(
                "{sig} requires at least one body part: text or html"
            )));
        }

        let cc = parse_mailbox_list(
            kwarg_or_arg(kwargs, args, "cc", 4).unwrap_or(Value::None),
            sig,
            "cc",
        )?;
        let bcc = parse_mailbox_list(
            kwarg_or_arg(kwargs, args, "bcc", 5).unwrap_or(Value::None),
            sig,
            "bcc",
        )?;
        let reply_to = parse_optional_mailbox(kwarg_or_arg(kwargs, args, "reply_to", 6), sig, "reply_to")?;

        let from_override = parse_optional_str_value(kwarg_or_arg(kwargs, args, "from", 7), sig, "from")?;
        let from = from_override
            .or_else(|| self.config.default_from.clone())
            .ok_or_else(|| {
                RelayError::Type(format!(
                    "{sig} requires from in Email(...) or send(...)"
                ))
            })?;
        let from = parse_mailbox(from, sig, "from")?;

        let headers = kwarg_or_arg(kwargs, args, "headers", 8)
            .map(|v| parse_headers_value(v, sig))
            .transpose()?
            .unwrap_or_default();
        let attachments = parse_email_attachments(kwarg_or_arg(kwargs, args, "attachments", 9), sig)?;

        let mut builder = MailMessage::builder().from(from).subject(subject);
        for mb in &to {
            builder = builder.to(mb.clone());
        }
        for mb in &cc {
            builder = builder.cc(mb.clone());
        }
        for mb in &bcc {
            builder = builder.bcc(mb.clone());
        }
        if let Some(reply_to) = reply_to {
            builder = builder.reply_to(reply_to);
        }

        for (name, value) in headers {
            let header_name = mail_header::HeaderName::new_from_ascii(name.clone())
                .map_err(|e| RelayError::Type(format!(
                    "{sig} invalid header name '{name}': {e}"
                )))?;
            builder = builder.raw_header(mail_header::HeaderValue::new(header_name, value));
        }

        let message = if attachments.is_empty() {
            match (text, html) {
                (Some(text), Some(html)) => builder
                    .multipart(
                        MultiPart::alternative()
                            .singlepart(SinglePart::plain(text))
                            .singlepart(SinglePart::html(html)),
                    )
                    .map_err(|e| RelayError::Runtime(format!("email message build failed: {e}")))?,
                (Some(text), None) => builder
                    .singlepart(SinglePart::plain(text))
                    .map_err(|e| RelayError::Runtime(format!("email message build failed: {e}")))?,
                (None, Some(html)) => builder
                    .singlepart(SinglePart::html(html))
                    .map_err(|e| RelayError::Runtime(format!("email message build failed: {e}")))?,
                (None, None) => {
                    return Err(RelayError::Type(format!(
                        "{sig} requires at least one body part: text or html"
                    )))
                }
            }
        } else {
            let mut mixed = MultiPart::mixed().build();
            mixed = match (text, html) {
                (Some(text), Some(html)) => mixed.multipart(
                    MultiPart::alternative()
                        .singlepart(SinglePart::plain(text))
                        .singlepart(SinglePart::html(html)),
                ),
                (Some(text), None) => mixed.singlepart(SinglePart::plain(text)),
                (None, Some(html)) => mixed.singlepart(SinglePart::html(html)),
                (None, None) => {
                    return Err(RelayError::Type(format!(
                        "{sig} requires at least one body part: text or html"
                    )))
                }
            };

            for attachment in attachments {
                let content_type = mail_header::ContentType::parse(&attachment.content_type)
                    .map_err(|e| {
                        RelayError::Type(format!(
                            "{sig} invalid attachment content_type '{}': {e}",
                            attachment.content_type
                        ))
                    })?;
                mixed = mixed.singlepart(
                    MailAttachment::new(attachment.filename).body(attachment.bytes, content_type),
                );
            }

            builder
                .multipart(mixed)
                .map_err(|e| RelayError::Runtime(format!("email message build failed: {e}")))?
        };

        let envelope_to = to
            .iter()
            .chain(cc.iter())
            .chain(bcc.iter())
            .map(|mb| mb.email.to_string())
            .collect::<Vec<_>>();

        Ok(PreparedEmail {
            message,
            envelope_to,
            message_id: None,
        })
    }

    fn get_member(&self, name: &str) -> Value {
        match name {
            "send" => {
                let email = self.clone();
                Value::Builtin(Arc::new(move |args, kwargs| {
                    let email = email.clone();
                    Box::pin(async move {
                        let prepared = email.prepare_send_request(&args, &kwargs)?;
                        let PreparedEmail {
                            message,
                            envelope_to,
                            message_id,
                        } = prepared;
                        let sender = email.sender.clone();
                        let host = email.config.host.clone();
                        let port = email.config.port;

                        let d = Deferred::new(Box::pin(async move {
                            let metadata = sender(message).await?;
                            let mut out = IndexMap::new();
                            out.insert("ok".into(), Value::Bool(true));
                            out.insert("transport".into(), Value::Str("smtp".into()));
                            out.insert("host".into(), Value::Str(host));
                            out.insert("port".into(), Value::Int(port as i64));
                            out.insert(
                                "message_id".into(),
                                metadata
                                    .message_id
                                    .or(message_id)
                                    .map(Value::Str)
                                    .unwrap_or(Value::None),
                            );
                            out.insert(
                                "envelope_to".into(),
                                Value::List(envelope_to.into_iter().map(Value::Str).collect()),
                            );
                            out.insert(
                                "smtp_code".into(),
                                metadata
                                    .smtp_code
                                    .map(Value::Int)
                                    .unwrap_or(Value::None),
                            );
                            out.insert(
                                "smtp_message".into(),
                                metadata
                                    .smtp_message
                                    .map(Value::Str)
                                    .unwrap_or(Value::None),
                            );
                            Ok(Value::Dict(out))
                        }));
                        Ok(Value::Deferred(Arc::new(d)))
                    })
                }))
            }
            "render" => {
                Value::Builtin(Arc::new(move |args, kwargs| {
                    Box::pin(async move {
                        let sig = "email.render(template, data=None)";
                        let template = kwarg_or_arg(&kwargs, &args, "template", 0)
                            .ok_or_else(|| RelayError::Type(format!("{sig} missing template")))?;
                        let template = expect_str_value(template, sig, "template")?;
                        let data = parse_template_locals(kwarg_or_arg(&kwargs, &args, "data", 1), sig)?;
                        let rendered = render_template(&template, &data)?;
                        Ok(Value::Str(rendered))
                    })
                }))
            }
            "render_file" => {
                Value::Builtin(Arc::new(move |args, kwargs| {
                    Box::pin(async move {
                        let sig = "email.render_file(path, data=None)";
                        let path = kwarg_or_arg(&kwargs, &args, "path", 0)
                            .ok_or_else(|| RelayError::Type(format!("{sig} missing path")))?;
                        let path = expect_str_value(path, sig, "path")?;
                        let data = parse_template_locals(kwarg_or_arg(&kwargs, &args, "data", 1), sig)?;

                        let d = Deferred::new(Box::pin(async move {
                            let template = tokio::fs::read_to_string(path)
                                .await
                                .map_err(|e| RelayError::Runtime(e.to_string()))?;
                            let rendered = render_template(&template, &data)?;
                            Ok(Value::Str(rendered))
                        }));
                        Ok(Value::Deferred(Arc::new(d)))
                    })
                }))
            }
            _ => Value::None,
        }
    }
}

#[derive(Clone)]
enum AuthStoreBackend {
    Memory(Arc<tokio::sync::Mutex<HashMap<String, String>>>),
    Callback {
        load_fn: String,
        save_fn: String,
        evaluator: Arc<Evaluator>,
    },
}

#[derive(Clone)]
struct AuthStoreHandle {
    backend: AuthStoreBackend,
}

impl AuthStoreHandle {
    fn memory() -> Self {
        Self {
            backend: AuthStoreBackend::Memory(Arc::new(tokio::sync::Mutex::new(HashMap::new()))),
        }
    }

    fn callback(evaluator: Arc<Evaluator>, load_fn: String, save_fn: String) -> Self {
        Self {
            backend: AuthStoreBackend::Callback {
                load_fn,
                save_fn,
                evaluator,
            },
        }
    }

    async fn load_hash(&self, username: String) -> RResult<Option<String>> {
        match &self.backend {
            AuthStoreBackend::Memory(store) => Ok(store.lock().await.get(&username).cloned()),
            AuthStoreBackend::Callback {
                load_fn,
                evaluator,
                ..
            } => {
                let out = evaluator
                    .call_named_function(load_fn, vec![Value::Str(username)])
                    .await?;
                match out {
                    Value::None => Ok(None),
                    Value::Str(s) => Ok(Some(s)),
                    other => Err(RelayError::Type(format!(
                        "auth load callback must return str/None, got {}",
                        value_type_name(&other)
                    ))),
                }
            }
        }
    }

    async fn save_hash(&self, username: String, hash: String) -> RResult<()> {
        match &self.backend {
            AuthStoreBackend::Memory(store) => {
                store.lock().await.insert(username, hash);
                Ok(())
            }
            AuthStoreBackend::Callback {
                save_fn,
                evaluator,
                ..
            } => {
                let _ = evaluator
                    .call_named_function(save_fn, vec![Value::Str(username), Value::Str(hash)])
                    .await?;
                Ok(())
            }
        }
    }

    fn get_member(&self, name: &str) -> Value {
        match name {
            "set_hash" => {
                let store = self.clone();
                Value::Builtin(Arc::new(move |args, _| {
                    let store = store.clone();
                    Box::pin(async move {
                        let username = expect_str(&args, 0, "auth_store.set_hash(username, hash)")?;
                        let hash = expect_str(&args, 1, "auth_store.set_hash(username, hash)")?;
                        store.save_hash(username, hash).await?;
                        Ok(Value::None)
                    })
                }))
            }
            "get_hash" => {
                let store = self.clone();
                Value::Builtin(Arc::new(move |args, _| {
                    let store = store.clone();
                    Box::pin(async move {
                        let username = expect_str(&args, 0, "auth_store.get_hash(username)")?;
                        match store.load_hash(username).await? {
                            Some(hash) => Ok(Value::Str(hash)),
                            None => Ok(Value::None),
                        }
                    })
                }))
            }
            "register" => {
                let store = self.clone();
                Value::Builtin(Arc::new(move |args, _| {
                    let store = store.clone();
                    Box::pin(async move {
                        let username =
                            expect_str(&args, 0, "auth_store.register(username, password)")?;
                        let password =
                            expect_str(&args, 1, "auth_store.register(username, password)")?;
                        let salt = SaltString::generate(&mut OsRng);
                        let hash = Argon2::default()
                            .hash_password(password.as_bytes(), &salt)
                            .map_err(|e| RelayError::Runtime(e.to_string()))?
                            .to_string();
                        store.save_hash(username, hash).await?;
                        Ok(Value::Bool(true))
                    })
                }))
            }
            "verify" => {
                let store = self.clone();
                Value::Builtin(Arc::new(move |args, _| {
                    let store = store.clone();
                    Box::pin(async move {
                        let username = expect_str(&args, 0, "auth_store.verify(username, password)")?;
                        let password = expect_str(&args, 1, "auth_store.verify(username, password)")?;
                        let Some(hash) = store.load_hash(username).await? else {
                            return Ok(Value::Bool(false));
                        };
                        let parsed = match PasswordHash::new(&hash) {
                            Ok(h) => h,
                            Err(_) => return Ok(Value::Bool(false)),
                        };
                        Ok(Value::Bool(
                            Argon2::default()
                                .verify_password(password.as_bytes(), &parsed)
                                .is_ok(),
                        ))
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
                        let filter = value_to_filter_document(
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
                        let filter = value_to_filter_document(
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
                        let filter = value_to_filter_document(
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
                        let filter = value_to_filter_document(
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
                        let filter = value_to_filter_document(
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
                "group" => {
                    let a = a.clone();
                    Value::Builtin(Arc::new(move |args, _| {
                        let a = a.clone();
                        Box::pin(async move {
                            let prefix = expect_str(&args, 0, "app.group(prefix)")?;
                            Ok(Value::Obj(Object::Route(a.group(prefix))))
                        })
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
                "render_template" => Value::Builtin(Arc::new(move |args, kwargs| {
                    Box::pin(async move {
                        let sig = "app.render_template(template, data=None)";
                        let template = kwarg_or_arg(&kwargs, &args, "template", 0)
                            .ok_or_else(|| RelayError::Type(format!("{sig} missing template")))?;
                        let template = expect_str_value(template, sig, "template")?;
                        let data = parse_template_locals(kwarg_or_arg(&kwargs, &args, "data", 1), sig)?;
                        let rendered = render_template(&template, &data)?;
                        Ok(Value::Str(rendered))
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
                "openapi" => {
                    let a = a.clone();
                    Value::Builtin(Arc::new(move |args, kwargs| {
                        let a = a.clone();
                        Box::pin(async move {
                            let title = kwargs
                                .iter()
                                .find(|(k, _)| k == "title")
                                .map(|(_, v)| v.clone())
                                .or_else(|| args.get(0).cloned())
                                .unwrap_or(Value::Str("Relay API".into()))
                                .repr();
                            let version = kwargs
                                .iter()
                                .find(|(k, _)| k == "version")
                                .map(|(_, v)| v.clone())
                                .or_else(|| args.get(1).cloned())
                                .unwrap_or(Value::Str("1.0.0".into()))
                                .repr();
                            a.set_openapi(OpenApiConfig { title, version });
                            Ok(Value::None)
                        })
                    }))
                }
                "session" => {
                    let a = a.clone();
                    Value::Builtin(Arc::new(move |args, kwargs| {
                        let a = a.clone();
                        Box::pin(async move {
                            let mut cfg = a.session_config();

                            if let Some(v) = args.get(0) {
                                cfg.secure = Some(bool_from_value(v, "app.session(secure=...)")?);
                            }
                            if let Some(v) = args.get(1) {
                                cfg.same_site = v.repr();
                            }

                            for (k, v) in kwargs {
                                match k.as_str() {
                                    "secure" => {
                                        cfg.secure =
                                            Some(bool_from_value(&v, "app.session(secure=...)")?)
                                    }
                                    "http_only" => {
                                        cfg.http_only =
                                            bool_from_value(&v, "app.session(http_only=...)")?
                                    }
                                    "same_site" => cfg.same_site = v.repr(),
                                    _ => {}
                                }
                            }

                            a.set_session_config(cfg);
                            Ok(Value::None)
                        })
                    }))
                }
                "uploads" => {
                    let a = a.clone();
                    Value::Builtin(Arc::new(move |args, kwargs| {
                        let a = a.clone();
                        Box::pin(async move {
                            let sig = "app.uploads(max_body_bytes=None, max_file_bytes=None, allowed_mime_types=None)";
                            let mut cfg = a.upload_config();

                            if let Some(v) = args.get(0) {
                                if !matches!(v, Value::None) {
                                    cfg.max_body_bytes =
                                        parse_usize_value(v.clone(), sig, "max_body_bytes")?;
                                }
                            }
                            if let Some(v) = args.get(1) {
                                if !matches!(v, Value::None) {
                                    cfg.max_file_bytes =
                                        parse_usize_value(v.clone(), sig, "max_file_bytes")?;
                                }
                            }
                            if let Some(v) = args.get(2) {
                                cfg.allowed_mime_types = parse_optional_mime_allowlist(
                                    Some(v.clone()),
                                    sig,
                                    "allowed_mime_types",
                                )?;
                            }

                            for (k, v) in kwargs {
                                match k.as_str() {
                                    "max_body_bytes" => {
                                        cfg.max_body_bytes =
                                            parse_usize_value(v, sig, "max_body_bytes")?
                                    }
                                    "max_file_bytes" => {
                                        cfg.max_file_bytes =
                                            parse_usize_value(v, sig, "max_file_bytes")?
                                    }
                                    "allowed_mime_types" | "allowed_mimes" => {
                                        cfg.allowed_mime_types = parse_optional_mime_allowlist(
                                            Some(v),
                                            sig,
                                            "allowed_mime_types",
                                        )?
                                    }
                                    _ => {}
                                }
                            }

                            a.set_upload_config(cfg);
                            Ok(Value::None)
                        })
                    }))
                }
                "session_backend" => {
                    let a = a.clone();
                    Value::Builtin(Arc::new(move |args, _| {
                        let a = a.clone();
                        Box::pin(async move {
                            let load_fn = args
                                .get(0)
                                .cloned()
                                .ok_or_else(|| {
                                    RelayError::Type(
                                        "app.session_backend(load_fn, save_fn) expects load_fn"
                                            .into(),
                                    )
                                })?;
                            let save_fn = args
                                .get(1)
                                .cloned()
                                .ok_or_else(|| {
                                    RelayError::Type(
                                        "app.session_backend(load_fn, save_fn) expects save_fn"
                                            .into(),
                                    )
                                })?;
                            let load_fn = match load_fn {
                                Value::Function(f) => f.name.clone(),
                                _ => {
                                    return Err(RelayError::Type(
                                        "app.session_backend(load_fn, save_fn) expects functions"
                                            .into(),
                                    ))
                                }
                            };
                            let save_fn = match save_fn {
                                Value::Function(f) => f.name.clone(),
                                _ => {
                                    return Err(RelayError::Type(
                                        "app.session_backend(load_fn, save_fn) expects functions"
                                            .into(),
                                    ))
                                }
                            };

                            a.set_session_backend(SessionBackendConfig::Callback {
                                load_fn,
                                save_fn,
                            });
                            Ok(Value::None)
                        })
                    }))
                }
                _ => Value::None,
            },
            Object::Route(r) => match name {
                "group" => {
                    let r = r.clone();
                    Value::Builtin(Arc::new(move |args, _| {
                        let r = r.clone();
                        Box::pin(async move {
                            let prefix = expect_str(&args, 0, "route.group(prefix)")?;
                            Ok(Value::Obj(Object::Route(r.group(prefix))))
                        })
                    }))
                }
                _ => Value::None,
            },
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
            Object::WebSocket(ws) => match name {
                "recv" => {
                    let ws = ws.clone();
                    Value::Builtin(Arc::new(move |_args, _| {
                        let ws = ws.clone();
                        Box::pin(async move {
                            let mut socket = ws.inner.lock().await;
                            match socket.next().await {
                                Some(Ok(WsMessage::Text(s))) => Ok(Value::Str(s.to_string())),
                                Some(Ok(WsMessage::Binary(b))) => Ok(Value::Bytes(b.to_vec())),
                                Some(Ok(WsMessage::Ping(_))) | Some(Ok(WsMessage::Pong(_))) => {
                                    Ok(Value::None)
                                }
                                Some(Ok(WsMessage::Close(_))) | None => Ok(Value::None),
                                Some(Err(e)) => Err(RelayError::Runtime(format!(
                                    "WebSocket receive error: {e}"
                                ))),
                            }
                        })
                    }))
                }
                "send" => {
                    let ws = ws.clone();
                    Value::Builtin(Arc::new(move |args, _| {
                        let ws = ws.clone();
                        Box::pin(async move {
                            let msg = args.get(0).cloned().unwrap_or(Value::None);
                            let frame = match msg {
                                Value::Bytes(b) => WsMessage::Binary(b.into()),
                                Value::None => WsMessage::Text(String::new().into()),
                                other => WsMessage::Text(other.repr().into()),
                            };
                            let mut socket = ws.inner.lock().await;
                            socket.send(frame).await.map_err(|e| {
                                RelayError::Runtime(format!("WebSocket send error: {e}"))
                            })?;
                            Ok(Value::None)
                        })
                    }))
                }
                "close" => {
                    let ws = ws.clone();
                    Value::Builtin(Arc::new(move |_args, _| {
                        let ws = ws.clone();
                        Box::pin(async move {
                            let mut socket = ws.inner.lock().await;
                            socket.send(WsMessage::Close(None)).await.map_err(|e| {
                                RelayError::Runtime(format!("WebSocket close error: {e}"))
                            })?;
                            Ok(Value::None)
                        })
                    }))
                }
                _ => Value::None,
            },
            Object::Http(h) => h.get_member(name),
            Object::HttpResponse(r) => r.get_member(name),
            Object::Email(e) => e.get_member(name),
            Object::AuthStore(a) => a.get_member(name),
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
            request_id,
            route_validation,
            path_params,
            query,
            json,
            form,
            headers,
            cookies,
            session_id,
            websocket,
        } = req;

        let query_map = query.clone();
        let mut json_body = json.clone();
        let headers_map = headers.clone();

        let mut query_values: IndexMap<String, Value> = query_map
            .iter()
            .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
            .collect();
        let mut form_values: IndexMap<String, Value> =
            form.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let mut json_values: Option<IndexMap<String, Value>> = json_body.as_ref().and_then(|j| {
            if let J::Object(obj) = j {
                Some(
                    obj.iter()
                        .map(|(k, v)| (k.clone(), json_to_value(v)))
                        .collect::<IndexMap<_, _>>(),
                )
            } else {
                None
            }
        });

        let validation_error = |msg: String| {
            Value::Response(error_response(
                400,
                "validation_error",
                &msg,
                None,
                Some(request_id.clone()),
            ))
        };

        if let Some(schema) = route_validation.query_schema {
            let validated = match validate_data_with_schema(
                Value::Dict(query_values.clone()),
                schema,
                "decorator query validate",
            ) {
                Ok(v) => v,
                Err(e) => return Ok(validation_error(e.to_string())),
            };
            query_values = expect_object_like(validated, "decorator query validate")?;
        }

        if let Some(schema) = route_validation.body_schema {
            let validated = match validate_data_with_schema(
                Value::Dict(form_values.clone()),
                schema,
                "decorator body validate",
            ) {
                Ok(v) => v,
                Err(e) => return Ok(validation_error(e.to_string())),
            };
            form_values = expect_object_like(validated, "decorator body validate")?;
        }

        if let Some(schema) = route_validation.json_schema {
            let Some(j) = json_body.clone() else {
                return Ok(validation_error("Missing JSON body".to_string()));
            };
            let validated =
                match validate_data_with_schema(Value::Json(j), schema, "decorator json validate") {
                    Ok(v) => v,
                    Err(e) => return Ok(validation_error(e.to_string())),
                };
            let map = expect_object_like(validated, "decorator json validate")?;
            json_body = Some(value_to_json(&Value::Dict(map.clone())));
            json_values = Some(map);
        }

        if let Some(schema) = route_validation.validate_schema {
            if let Some(j) = json_body.clone() {
                let validated = match validate_data_with_schema(
                    Value::Json(j),
                    schema,
                    "decorator validate",
                ) {
                    Ok(v) => v,
                    Err(e) => return Ok(validation_error(e.to_string())),
                };
                let map = expect_object_like(validated, "decorator validate")?;
                json_body = Some(value_to_json(&Value::Dict(map.clone())));
                json_values = Some(map);
            } else {
                let validated = match validate_data_with_schema(
                    Value::Dict(form_values.clone()),
                    schema,
                    "decorator validate",
                ) {
                    Ok(v) => v,
                    Err(e) => return Ok(validation_error(e.to_string())),
                };
                form_values = expect_object_like(validated, "decorator validate")?;
            }
        }

        // build arg map by param list
        let mut bound: HashMap<String, Value> = HashMap::new();

        if let Some(ws) = websocket.clone() {
            bound.insert("socket".into(), Value::Obj(Object::WebSocket(ws)));
        }

        // start with Query
        for (k, v) in &query_values {
            bound.insert(k.clone(), v.clone());
        }
        // then Body
        for (k, v) in &form_values {
            bound.insert(k.clone(), v.clone());
        }
        if let Some(j) = json_body.clone() {
            // JSON object keys are treated like body params for handler arg binding.
            if let Some(obj_values) = &json_values {
                for (k, v) in obj_values {
                    bound.insert(k.clone(), v.clone());
                }
            }

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
        req_dict.insert("request_id".into(), Value::Str(request_id.clone()));
        req_dict.insert(
            "query".into(),
            Value::Dict(query_values.clone()),
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
        if let Some(j) = json_body.clone() {
            req_dict.insert("json".into(), Value::Json(j));
        }
        req_dict.insert(
            "form".into(),
            Value::Dict(form_values.clone()),
        );

        let existing_session = self.load_session_data(&app, &session_id).await?;
        let session_cfg = app.session_config();

        // run handler in new scope
        {
            let mut env = self.env.lock().await;
            env.push();
            for (k, v) in &bound {
                env.set(k, v.clone());
            }
            env.set("request", Value::Dict(req_dict.clone()));
            env.set(
                "cookies",
                Value::Dict(
                    cookies
                        .iter()
                        .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
                        .collect(),
                ),
            );
            if let Some(ws) = websocket.clone() {
                env.set("socket", Value::Obj(Object::WebSocket(ws)));
            }
            env.set("session", Value::Dict(existing_session.clone()));
        }

        let mut middlewares = Vec::new();
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
            middlewares.push(mw_func);
        }

        let evaluator = Arc::new(self.clone());
        let handler_fn = func.clone();
        let handler: Arc<dyn Fn() -> BoxFut + Send + Sync> = Arc::new(move || {
            let evaluator = evaluator.clone();
            let handler_fn = handler_fn.clone();
            Box::pin(async move {
                match evaluator.eval_block(&handler_fn.body).await? {
                    Flow::None => Ok(Value::None),
                    Flow::Return(v) => Ok(v),
                }
            })
        });

        let chain_out = if middlewares.is_empty() {
            (handler)().await
        } else {
            Arc::new(self.clone())
                .run_middleware_chain(
                    Arc::new(middlewares),
                    0,
                    Value::Dict(req_dict.clone()),
                    handler,
                )
                .await
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

        let out = chain_out?;

        if websocket.is_some() {
            if let Some(sid) = session_id {
                self.persist_session_data(&app, sid, final_session).await?;
            }
            return Ok(Value::None);
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
            self.persist_session_data(&app, sid.clone(), final_session)
                .await?;
            response
                .set_cookies
                .push(build_session_cookie(&sid, &session_cfg, &headers_map));
        }
        response
            .headers
            .insert("x-request-id".into(), request_id.clone());

        Ok(Value::Response(response))
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_src(src: &str) -> RResult<Program> {
        let mut lexer = Lexer::new(src);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser::new(tokens);
        parser.parse_program()
    }

    #[test]
    fn parses_if_else_block_in_function() {
        let src = r#"fn example()
    x = 10
    if (x > 5)
        print("x is greater than 5")
    else
        print("x is 5 or less")
"#;

        let program = parse_src(src).expect("if/else should parse");
        assert_eq!(program.stmts.len(), 1);

        let Stmt::FuncDef { body, .. } = &program.stmts[0] else {
            panic!("expected function definition");
        };
        assert_eq!(body.len(), 2);
        assert!(matches!(body[1], Stmt::If { .. }));

        let Stmt::If { else_block, .. } = &body[1] else {
            panic!("expected if statement");
        };
        assert_eq!(else_block.len(), 1, "else block should be attached to if");
    }

    #[test]
    fn reports_dangling_else() {
        let src = r#"fn example()
    x = 10
    else
        print("dangling")
"#;

        let err = parse_src(src).expect_err("dangling else should fail");
        assert!(matches!(
            err,
            RelayError::Syntax { msg, .. } if msg.contains("without matching if")
        ));
    }

    #[test]
    fn bson_object_id_is_exposed_as_plain_string() {
        let oid = ObjectId::parse_str("698d35e0ef97187ab267d11e").expect("valid object id");
        let json = bson_to_json(Bson::ObjectId(oid)).expect("conversion should succeed");
        assert_eq!(json, J::String("698d35e0ef97187ab267d11e".to_string()));
    }

    #[test]
    fn filter_document_coerces_id_strings_to_object_ids() {
        let oid = "698d35e0ef97187ab267d11e";
        let filter = Value::Json(serde_json::json!({
            "_id": { "$in": [oid] },
            "$or": [{ "_id": oid }]
        }));

        let doc =
            value_to_filter_document(filter, "collection.find_one(filter)").expect("valid filter");

        let top_id = doc
            .get_document("_id")
            .expect("top-level _id document")
            .get_array("$in")
            .expect("$in array")
            .first()
            .expect("first $in value");
        assert!(matches!(top_id, Bson::ObjectId(_)));

        let nested_id = doc
            .get_array("$or")
            .expect("$or array")
            .first()
            .expect("first $or clause")
            .as_document()
            .expect("$or clause should be a document")
            .get("_id")
            .expect("nested _id");
        assert!(matches!(nested_id, Bson::ObjectId(_)));
    }

    #[test]
    fn value_to_document_accepts_oid_extended_json() {
        let input = Value::Json(serde_json::json!({
            "_id": { "$oid": "698d35e0ef97187ab267d11e" }
        }));

        let doc = value_to_document(input, "test").expect("document should parse");
        let id = doc.get("_id").expect("_id should exist");
        assert!(
            matches!(id, Bson::ObjectId(_)),
            "value_to_document should coerce $oid extended JSON"
        );
    }

    #[tokio::test]
    async fn parses_urlencoded_form_request_body() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/x-www-form-urlencoded"),
        );

        let upload_cfg = UploadConfig::default();
        let (json, form) = parse_request_body(
            &headers,
            Bytes::from_static(b"content=hello+world&tag=notes"),
            &upload_cfg,
        )
        .await
        .expect("urlencoded parse should succeed");
        assert!(json.is_none());
        assert!(
            matches!(form.get("content"), Some(Value::Str(s)) if s == "hello world"),
            "content should be URL-decoded"
        );
        assert!(matches!(form.get("tag"), Some(Value::Str(s)) if s == "notes"));
    }

    #[tokio::test]
    async fn parses_multipart_form_request_body_with_file_uploads() {
        let boundary = "relay-boundary";
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_str(&format!(
                "multipart/form-data; boundary={boundary}"
            ))
            .expect("header should be valid"),
        );

        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"title\"\r\n\r\nUpload Test\r\n\
--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"hello.txt\"\r\nContent-Type: text/plain\r\n\r\nhello world\r\n\
--{boundary}--\r\n"
        );

        let upload_cfg = UploadConfig::default();
        let (json, form) = parse_request_body(&headers, Bytes::from(body), &upload_cfg)
            .await
            .expect("multipart parse should succeed");
        assert!(json.is_none());
        assert!(matches!(
            form.get("title"),
            Some(Value::Str(s)) if s == "Upload Test"
        ));

        let file = form.get("file").expect("file field should exist");
        match file {
            Value::Dict(meta) => {
                assert!(matches!(
                    meta.get("filename"),
                    Some(Value::Str(s)) if s == "hello.txt"
                ));
                assert!(matches!(
                    meta.get("content_type"),
                    Some(Value::Str(s)) if s == "text/plain"
                ));
                assert!(matches!(meta.get("size"), Some(Value::Int(11))));
                assert!(matches!(
                    meta.get("bytes"),
                    Some(Value::Bytes(bytes)) if bytes == b"hello world"
                ));
            }
            other => panic!("file field should be dict, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn request_body_limit_rejects_oversized_payloads() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        );
        let upload_cfg = UploadConfig {
            max_body_bytes: 4,
            ..UploadConfig::default()
        };

        let err = parse_request_body(&headers, Bytes::from_static(b"{\"x\":1}"), &upload_cfg)
            .await;
        let err = match err {
            Ok(_) => panic!("body larger than limit should fail"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            RelayError::Type(msg) if msg.contains("max_body_bytes")
        ));
    }

    #[tokio::test]
    async fn multipart_file_limit_rejects_oversized_file() {
        let boundary = "relay-boundary-limit";
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_str(&format!(
                "multipart/form-data; boundary={boundary}"
            ))
            .expect("header should be valid"),
        );
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"big.txt\"\r\nContent-Type: text/plain\r\n\r\nhello world\r\n--{boundary}--\r\n"
        );
        let upload_cfg = UploadConfig {
            max_file_bytes: 5,
            ..UploadConfig::default()
        };

        let err = parse_request_body(&headers, Bytes::from(body), &upload_cfg)
            .await;
        let err = match err {
            Ok(_) => panic!("file larger than max_file_bytes should fail"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            RelayError::Type(msg) if msg.contains("max_file_bytes")
        ));
    }

    #[tokio::test]
    async fn multipart_mime_allowlist_rejects_disallowed_file_type() {
        let boundary = "relay-boundary-mime";
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_str(&format!(
                "multipart/form-data; boundary={boundary}"
            ))
            .expect("header should be valid"),
        );
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"hello.txt\"\r\nContent-Type: text/plain\r\n\r\nhello world\r\n--{boundary}--\r\n"
        );
        let upload_cfg = UploadConfig {
            allowed_mime_types: Some(HashSet::from(["image/png".to_string()])),
            ..UploadConfig::default()
        };

        let err = parse_request_body(&headers, Bytes::from(body), &upload_cfg)
            .await;
        let err = match err {
            Ok(_) => panic!("disallowed mime should fail"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            RelayError::Type(msg) if msg.contains("not allowed")
        ));
    }

    #[tokio::test]
    async fn webapp_uploads_method_updates_upload_config() {
        let src = r#"app = WebApp()
app.uploads(
    max_body_bytes=2048,
    max_file_bytes=512,
    allowed_mime_types=["image/png", "application/pdf"]
)
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let app = {
            let env_lock = env.lock().await;
            match env_lock.get("app").expect("app should exist") {
                Value::Obj(Object::WebApp(app)) => app,
                other => panic!("expected app to be WebApp, got {}", other.repr()),
            }
        };

        let cfg = app.upload_config();
        assert_eq!(cfg.max_body_bytes, 2048);
        assert_eq!(cfg.max_file_bytes, 512);
        let allowed = cfg.allowed_mime_types.expect("allowlist should be set");
        assert!(allowed.contains("image/png"));
        assert!(allowed.contains("application/pdf"));
    }

    #[tokio::test]
    async fn function_type_hints_require_exact_runtime_types() {
        let src = r#"fn add(a: int, b: int)
    return a + b

add("5", "10")
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }

        let err = match evaluator.eval_program(&program).await {
            Ok(v) => panic!(
                "typed function should reject string args for int params, got {}",
                v.repr()
            ),
            Err(e) => e,
        };
        assert!(
            matches!(err, RelayError::Type(ref msg) if msg.contains("Expected int, got str")),
            "expected strict int validation error, got: {err}"
        );
    }

    #[tokio::test]
    async fn function_type_hints_reject_unknown_types() {
        let src = r#"fn greet(name: UserId)
    return name

greet("ada")
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }

        let err = match evaluator.eval_program(&program).await {
            Ok(v) => panic!("unknown type hints should fail fast, got {}", v.repr()),
            Err(e) => e,
        };
        assert!(
            matches!(err, RelayError::Type(ref msg) if msg.contains("Unknown type hint: UserId")),
            "expected unknown type hint error, got: {err}"
        );
    }

    #[tokio::test]
    async fn web_handler_prefers_form_body_over_query() {
        let src = r#"fn submit(content)
    return content
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let mut query = HashMap::new();
        query.insert("content".to_string(), "from-query".to_string());
        let mut form = HashMap::new();
        form.insert("content".to_string(), Value::Str("from-form".to_string()));

        let out = evaluator
            .call_web_handler(
                WebAppHandle::new(),
                "submit",
                RequestParts {
                    method: "POST".to_string(),
                    path: "/".to_string(),
                    request_id: "test-rid".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query,
                    json: None,
                    form,
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("handler should run");

        match out {
            Value::Response(resp) => {
                let text = String::from_utf8(resp.body).expect("response body should be utf-8");
                assert_eq!(text, "from-form");
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn web_handler_type_hints_still_coerce_query_values() {
        let src = r#"fn search(limit: int)
    return str(limit)
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let mut query = HashMap::new();
        query.insert("limit".to_string(), "10".to_string());

        let out = evaluator
            .call_web_handler(
                WebAppHandle::new(),
                "search",
                RequestParts {
                    method: "GET".to_string(),
                    path: "/search".to_string(),
                    request_id: "test-rid".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query,
                    json: None,
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("handler should run");

        match out {
            Value::Response(resp) => {
                let text = String::from_utf8(resp.body).expect("response body should be utf-8");
                assert_eq!(text, "10");
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn web_handler_binds_json_object_keys_to_args() {
        let src = r#"fn submit(name, age: int = 0)
    return name + ":" + str(age)
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let mut query = HashMap::new();
        query.insert("name".to_string(), "from-query".to_string());

        let out = evaluator
            .call_web_handler(
                WebAppHandle::new(),
                "submit",
                RequestParts {
                    method: "POST".to_string(),
                    path: "/".to_string(),
                    request_id: "test-rid".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query,
                    json: Some(serde_json::json!({
                        "name": "from-json",
                        "age": 33
                    })),
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("handler should run");

        match out {
            Value::Response(resp) => {
                let text = String::from_utf8(resp.body).expect("response body should be utf-8");
                assert_eq!(text, "from-json:33");
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn web_handler_helpers_work_without_handler_args() {
        let src = r#"fn from_json()
    payload = get_json()
    return str(payload["name"])

fn from_form()
    payload = get_body()
    return payload["name"]

fn from_query()
    payload = get_query()
    return payload["name"]
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let out_json = evaluator
            .call_web_handler(
                WebAppHandle::new(),
                "from_json",
                RequestParts {
                    method: "POST".to_string(),
                    path: "/".to_string(),
                    request_id: "test-rid".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query: HashMap::new(),
                    json: Some(serde_json::json!({
                        "name": "from-json-helper"
                    })),
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("json helper handler should run");

        match out_json {
            Value::Response(resp) => {
                let text = String::from_utf8(resp.body).expect("response body should be utf-8");
                assert_eq!(text, "from-json-helper");
            }
            other => panic!("expected response, got {}", other.repr()),
        }

        let mut form = HashMap::new();
        form.insert("name".to_string(), Value::Str("from-form-helper".to_string()));
        let out_form = evaluator
            .call_web_handler(
                WebAppHandle::new(),
                "from_form",
                RequestParts {
                    method: "POST".to_string(),
                    path: "/".to_string(),
                    request_id: "test-rid".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query: HashMap::new(),
                    json: None,
                    form,
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("form helper handler should run");

        match out_form {
            Value::Response(resp) => {
                let text = String::from_utf8(resp.body).expect("response body should be utf-8");
                assert_eq!(text, "from-form-helper");
            }
            other => panic!("expected response, got {}", other.repr()),
        }

        let mut query = HashMap::new();
        query.insert("name".to_string(), "from-query-helper".to_string());
        let out_query = evaluator
            .call_web_handler(
                WebAppHandle::new(),
                "from_query",
                RequestParts {
                    method: "GET".to_string(),
                    path: "/".to_string(),
                    request_id: "test-rid".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query,
                    json: None,
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("query helper handler should run");

        match out_query {
            Value::Response(resp) => {
                let text = String::from_utf8(resp.body).expect("response body should be utf-8");
                assert_eq!(text, "from-query-helper");
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn web_handler_can_access_multipart_uploaded_file_metadata() {
        let src = r#"fn upload(file)
    return file["filename"] + ":" + str(file["size"])
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let boundary = "relay-boundary-upload";
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_str(&format!(
                "multipart/form-data; boundary={boundary}"
            ))
            .expect("header should be valid"),
        );
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"hello.txt\"\r\nContent-Type: text/plain\r\n\r\nhello world\r\n--{boundary}--\r\n"
        );
        let upload_cfg = UploadConfig::default();
        let (_json, form) = parse_request_body(&headers, Bytes::from(body), &upload_cfg)
            .await
            .expect("multipart parse should succeed");

        let out = evaluator
            .call_web_handler(
                WebAppHandle::new(),
                "upload",
                RequestParts {
                    method: "POST".to_string(),
                    path: "/upload".to_string(),
                    request_id: "rid-upload".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query: HashMap::new(),
                    json: None,
                    form,
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("handler should run");

        match out {
            Value::Response(resp) => {
                let text = String::from_utf8(resp.body).expect("response body should be utf-8");
                assert_eq!(text, "hello.txt:11");
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn webapp_render_template_method_renders_explicit_template() {
        let src = r#"app = WebApp()
rendered = app.render_template("Hello {{ name }}", {"name": "Ada"})
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let env_lock = env.lock().await;
        assert!(matches!(
            env_lock.get("rendered").expect("rendered should exist"),
            Value::Str(s) if s == "Hello Ada"
        ));
    }

    #[tokio::test]
    async fn http_error_builtin_returns_structured_json_and_request_id() {
        let src = r#"fn fail()
    return HTTPError(400, "bad_request", "Missing field", {"field": "name"})
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let out = evaluator
            .call_web_handler(
                WebAppHandle::new(),
                "fail",
                RequestParts {
                    method: "GET".to_string(),
                    path: "/fail".to_string(),
                    request_id: "rid-test".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query: HashMap::new(),
                    json: None,
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("handler should run");

        match out {
            Value::Response(resp) => {
                assert_eq!(resp.status, 400);
                assert_eq!(
                    resp.headers.get("x-request-id").map(String::as_str),
                    Some("rid-test")
                );
                let body = String::from_utf8(resp.body).expect("response body should be utf-8");
                let parsed: J = serde_json::from_str(&body).expect("body should be json");
                assert_eq!(parsed["error"]["code"], J::String("bad_request".to_string()));
                assert_eq!(
                    parsed["error"]["message"],
                    J::String("Missing field".to_string())
                );
                assert_eq!(
                    parsed["error"]["request_id"],
                    J::String("rid-test".to_string())
                );
                assert_eq!(parsed["error"]["details"]["field"], J::String("name".to_string()));
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn web_handler_sets_request_id_header_on_success() {
        let src = r#"fn ok()
    return "ok"
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let out = evaluator
            .call_web_handler(
                WebAppHandle::new(),
                "ok",
                RequestParts {
                    method: "GET".to_string(),
                    path: "/ok".to_string(),
                    request_id: "rid-success".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query: HashMap::new(),
                    json: None,
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("handler should run");

        match out {
            Value::Response(resp) => {
                assert_eq!(
                    resp.headers.get("x-request-id").map(String::as_str),
                    Some("rid-success")
                );
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn middleware_supports_ctx_and_next_params() {
        let src = r#"app = WebApp()

fn mw(ctx, next)
    if (ctx["path"] == "/hello")
        session["via"] = "middleware"
    next()

app.use(mw)

fn handler()
    return session["via"]
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let app = {
            let env_lock = env.lock().await;
            match env_lock.get("app").expect("app should exist") {
                Value::Obj(Object::WebApp(app)) => app,
                other => panic!("expected app to be WebApp, got {}", other.repr()),
            }
        };

        let out = evaluator
            .call_web_handler(
                app,
                "handler",
                RequestParts {
                    method: "GET".to_string(),
                    path: "/hello".to_string(),
                    request_id: "rid-mw".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query: HashMap::new(),
                    json: None,
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("handler should run");

        match out {
            Value::Response(resp) => {
                let text = String::from_utf8(resp.body).expect("response body should be utf-8");
                assert_eq!(text, "middleware");
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn middleware_next_chain_can_wrap_handler_result() {
        let src = r#"app = WebApp()

fn mw1(ctx, next)
    session["trace"] = "a"
    out = next()
    return "m1(" + str(out) + ")"

fn mw2(ctx, next)
    session["trace"] = session["trace"] + "b"
    out = next()
    return session["trace"] + ":" + str(out)

app.use(mw1)
app.use(mw2)

fn handler()
    return "h"
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let app = {
            let env_lock = env.lock().await;
            match env_lock.get("app").expect("app should exist") {
                Value::Obj(Object::WebApp(app)) => app,
                other => panic!("expected app to be WebApp, got {}", other.repr()),
            }
        };

        let out = evaluator
            .call_web_handler(
                app,
                "handler",
                RequestParts {
                    method: "GET".to_string(),
                    path: "/chain".to_string(),
                    request_id: "rid-mw-chain".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query: HashMap::new(),
                    json: None,
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("handler should run");

        match out {
            Value::Response(resp) => {
                let body = String::from_utf8(resp.body).expect("response body should be utf-8");
                assert_eq!(body, "m1(ab:h)");
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn middleware_next_result_is_used_when_not_returned_explicitly() {
        let src = r#"app = WebApp()

fn mw(ctx, next)
    next()

app.use(mw)

fn handler()
    return "ok"
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let app = {
            let env_lock = env.lock().await;
            match env_lock.get("app").expect("app should exist") {
                Value::Obj(Object::WebApp(app)) => app,
                other => panic!("expected app to be WebApp, got {}", other.repr()),
            }
        };

        let out = evaluator
            .call_web_handler(
                app,
                "handler",
                RequestParts {
                    method: "GET".to_string(),
                    path: "/chain".to_string(),
                    request_id: "rid-mw-chain-2".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query: HashMap::new(),
                    json: None,
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("handler should run");

        match out {
            Value::Response(resp) => {
                let body = String::from_utf8(resp.body).expect("response body should be utf-8");
                assert_eq!(body, "ok");
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn decorator_query_schema_is_enforced_at_runtime() {
        let src = r#"app = WebApp()
schema = {"limit": "int"}

@app.get("/search", query=schema)
fn search(limit)
    return str(limit)
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let app = {
            let env_lock = env.lock().await;
            match env_lock.get("app").expect("app should exist") {
                Value::Obj(Object::WebApp(app)) => app,
                other => panic!("expected app to be WebApp, got {}", other.repr()),
            }
        };
        let route_validation = {
            let state = app.inner.lock().unwrap();
            state
                .routes
                .first()
                .expect("route should be registered")
                .validation
                .clone()
        };

        let mut good_query = HashMap::new();
        good_query.insert("limit".to_string(), "10".to_string());
        let good = evaluator
            .call_web_handler(
                app.clone(),
                "search",
                RequestParts {
                    method: "GET".to_string(),
                    path: "/search".to_string(),
                    request_id: "rid-schema-good".to_string(),
                    route_validation: route_validation.clone(),
                    path_params: HashMap::new(),
                    query: good_query,
                    json: None,
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("good request should run");
        match good {
            Value::Response(resp) => {
                let body = String::from_utf8(resp.body).expect("response body should be utf-8");
                assert_eq!(body, "10");
            }
            other => panic!("expected response, got {}", other.repr()),
        }

        let mut bad_query = HashMap::new();
        bad_query.insert("limit".to_string(), "bad".to_string());
        let bad = evaluator
            .call_web_handler(
                app,
                "search",
                RequestParts {
                    method: "GET".to_string(),
                    path: "/search".to_string(),
                    request_id: "rid-schema-bad".to_string(),
                    route_validation,
                    path_params: HashMap::new(),
                    query: bad_query,
                    json: None,
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("bad request should return validation response");
        match bad {
            Value::Response(resp) => {
                assert_eq!(resp.status, 400);
                let body = String::from_utf8(resp.body).expect("response body should be utf-8");
                let parsed: J = serde_json::from_str(&body).expect("body should be json");
                assert_eq!(
                    parsed["error"]["code"],
                    J::String("validation_error".to_string())
                );
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn session_backend_callbacks_can_persist_across_requests() {
        let src = r#"session_db = {}
app = WebApp()

fn load_session(sid)
    return session_db[sid]

fn save_session(sid, data)
    session_db[sid] = data

app.session_backend(load_session, save_session)

fn set_user()
    session["user"] = "ada"
    return "ok"

fn get_user()
    return session["user"]
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let app = {
            let env_lock = env.lock().await;
            match env_lock.get("app").expect("app should exist") {
                Value::Obj(Object::WebApp(app)) => app,
                other => panic!("expected app to be WebApp, got {}", other.repr()),
            }
        };

        let out_set = evaluator
            .call_web_handler(
                app.clone(),
                "set_user",
                RequestParts {
                    method: "GET".to_string(),
                    path: "/set".to_string(),
                    request_id: "rid-session-a".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query: HashMap::new(),
                    json: None,
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("set handler should run");

        let sid = match out_set {
            Value::Response(resp) => {
                let cookie = resp
                    .set_cookies
                    .first()
                    .expect("session cookie should be set")
                    .clone();
                cookie
                    .split(';')
                    .next()
                    .and_then(|kv| kv.split_once('='))
                    .map(|(_, v)| v.to_string())
                    .expect("session cookie should have sid")
            }
            other => panic!("expected response, got {}", other.repr()),
        };

        let out_get = evaluator
            .call_web_handler(
                app,
                "get_user",
                RequestParts {
                    method: "GET".to_string(),
                    path: "/get".to_string(),
                    request_id: "rid-session-b".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query: HashMap::new(),
                    json: None,
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: Some(sid),
                    websocket: None,
                },
            )
            .await
            .expect("get handler should run");

        match out_get {
            Value::Response(resp) => {
                let body = String::from_utf8(resp.body).expect("response body should be utf-8");
                assert_eq!(body, "ada");
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn auth_hash_and_store_backends_work() {
        let src = r#"store = AuthStore()
h = auth_hash_password("secret")
ok = auth_verify_password("secret", h)
bad = auth_verify_password("wrong", h)

store.register("ada", "pw")
store_ok = store.verify("ada", "pw")
store_bad = store.verify("ada", "nope")

auth_db = {}
fn load_user(name)
    return auth_db[name]

fn save_user(name, hash)
    auth_db[name] = hash

custom = AuthStore(load_user, save_user)
custom.register("bob", "pw2")
custom_ok = custom.verify("bob", "pw2")
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let env_lock = env.lock().await;
        assert!(matches!(env_lock.get("ok").expect("ok"), Value::Bool(true)));
        assert!(matches!(env_lock.get("bad").expect("bad"), Value::Bool(false)));
        assert!(matches!(
            env_lock.get("store_ok").expect("store_ok"),
            Value::Bool(true)
        ));
        assert!(matches!(
            env_lock.get("store_bad").expect("store_bad"),
            Value::Bool(false)
        ));
        assert!(matches!(
            env_lock.get("custom_ok").expect("custom_ok"),
            Value::Bool(true)
        ));
    }

    #[tokio::test]
    async fn validate_helper_coerces_and_requires_fields() {
        let src = r#"result = validate({"limit": "10"}, {"limit": "int"})
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let env_lock = env.lock().await;
        match env_lock.get("result").expect("result should exist") {
            Value::Dict(map) => {
                assert!(matches!(map.get("limit"), Some(Value::Int(10))));
            }
            other => panic!("expected dict, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn require_query_validates_handler_input() {
        let src = r#"fn search()
    params = require_query({"limit": "int"})
    return str(params["limit"])
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let mut query = HashMap::new();
        query.insert("limit".to_string(), "25".to_string());

        let out = evaluator
            .call_web_handler(
                WebAppHandle::new(),
                "search",
                RequestParts {
                    method: "GET".to_string(),
                    path: "/search".to_string(),
                    request_id: "rid-validate".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query,
                    json: None,
                    form: HashMap::new(),
                    headers: HashMap::new(),
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("handler should run");

        match out {
            Value::Response(resp) => {
                let body = String::from_utf8(resp.body).expect("response body should be utf-8");
                assert_eq!(body, "25");
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn session_cookie_is_secure_for_https_requests() {
        let src = r#"fn handler()
    session["user"] = "ada"
    return "ok"
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let mut headers = HashMap::new();
        headers.insert("x-forwarded-proto".to_string(), "https".to_string());

        let out = evaluator
            .call_web_handler(
                WebAppHandle::new(),
                "handler",
                RequestParts {
                    method: "GET".to_string(),
                    path: "/".to_string(),
                    request_id: "rid-cookie".to_string(),
                    route_validation: RouteValidation::default(),
                    path_params: HashMap::new(),
                    query: HashMap::new(),
                    json: None,
                    form: HashMap::new(),
                    headers,
                    cookies: HashMap::new(),
                    session_id: None,
                    websocket: None,
                },
            )
            .await
            .expect("handler should run");

        match out {
            Value::Response(resp) => {
                let cookie = resp
                    .set_cookies
                    .first()
                    .expect("session write should set cookie");
                assert!(cookie.contains("Secure"));
                assert!(cookie.contains("HttpOnly"));
                assert!(cookie.contains("SameSite=Lax"));
            }
            other => panic!("expected response, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn route_groups_prefix_registered_paths() {
        let src = r#"app = WebApp()
api = app.group("/api")
v1 = api.group("/v1")

@v1.get("/users/<user_id>")
fn get_user(user_id)
    return user_id
"#;
        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }
        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let app = {
            let env_lock = env.lock().await;
            match env_lock.get("app").expect("app should exist") {
                Value::Obj(Object::WebApp(app)) => app,
                other => panic!("expected app to be WebApp, got {}", other.repr()),
            }
        };

        let state = app.inner.lock().unwrap();
        let route = state.routes.first().expect("one route should be registered");
        assert_eq!(route.path, "/api/v1/users/<user_id>");
    }

    #[test]
    fn openapi_doc_builder_emits_route_paths() {
        let doc = build_openapi_doc(
            &[RouteSpec {
                method: "GET".to_string(),
                path: "/users/<id>".to_string(),
                fn_name: "get_user".to_string(),
                validation: RouteValidation::default(),
            }],
            "Relay Test API",
            "2026.1",
        );

        assert_eq!(doc["info"]["title"], J::String("Relay Test API".to_string()));
        assert_eq!(doc["info"]["version"], J::String("2026.1".to_string()));
        assert!(doc["paths"]["/users/{id}"]["get"].is_object());
    }

    #[test]
    fn http_client_exposes_parity_methods_and_headers_member() {
        let http = HttpHandle::new();
        assert!(!matches!(http.get_member("put"), Value::None));
        assert!(!matches!(http.get_member("patch"), Value::None));
        assert!(!matches!(http.get_member("delete"), Value::None));

        let mut headers = HashMap::new();
        headers.insert("x-test".to_string(), "ok".to_string());
        let resp = HttpResponseHandle {
            status: 200,
            headers,
            body: b"ok".to_vec(),
        };

        match resp.get_member("headers") {
            Value::Dict(h) => {
                assert!(matches!(h.get("x-test"), Some(Value::Str(v)) if v == "ok"));
            }
            other => panic!("headers should be dict, got {}", other.repr()),
        }
    }

    #[test]
    fn email_constructor_rejects_invalid_tls_mode() {
        let err = match EmailHandle::from_constructor_args(
            &[Value::Str("smtp.example.com".into())],
            &[("tls".into(), Value::Str("bad-mode".into()))],
        ) {
            Ok(_) => panic!("invalid tls mode should fail"),
            Err(err) => err,
        };

        assert!(matches!(
            err,
            RelayError::Type(msg) if msg.contains("expects one of")
        ));
    }

    #[test]
    fn email_client_exposes_send_and_template_methods() {
        let email = EmailHandle::new(EmailConfig {
            host: "smtp.example.com".into(),
            port: 587,
            username: None,
            password: None,
            default_from: Some("Relay <noreply@example.com>".into()),
            tls_mode: EmailTlsMode::StartTls,
        });

        assert!(!matches!(email.get_member("send"), Value::None));
        assert!(!matches!(email.get_member("render"), Value::None));
        assert!(!matches!(email.get_member("render_file"), Value::None));
    }

    #[tokio::test]
    async fn email_send_returns_deferred_and_metadata_with_mock_sender() {
        let email = EmailHandle::with_sender_for_test(
            EmailConfig {
                host: "smtp.example.com".into(),
                port: 587,
                username: None,
                password: None,
                default_from: Some("Relay <noreply@example.com>".into()),
                tls_mode: EmailTlsMode::StartTls,
            },
            Arc::new(|_msg| {
                Box::pin(async move {
                    Ok(EmailSendMetadata {
                        message_id: Some("<msg-123@example.com>".into()),
                        smtp_code: Some(250),
                        smtp_message: Some("2.0.0 queued".into()),
                    })
                })
            }),
        );

        let send_fn = match email.get_member("send") {
            Value::Builtin(f) => f,
            other => panic!("send should be builtin, got {}", other.repr()),
        };

        let out = send_fn(
            vec![
                Value::Str("ada@example.com".into()),
                Value::Str("Welcome".into()),
                Value::Str("Hello Ada".into()),
            ],
            vec![],
        )
        .await
        .expect("send call should succeed");

        let deferred = match out {
            Value::Deferred(d) => d,
            other => panic!("send should return Deferred, got {}", other.repr()),
        };
        let resolved = deferred.resolve().await.expect("deferred should resolve");

        match resolved {
            Value::Dict(map) => {
                assert!(matches!(map.get("ok"), Some(Value::Bool(true))));
                assert!(matches!(
                    map.get("transport"),
                    Some(Value::Str(s)) if s == "smtp"
                ));
                assert!(matches!(
                    map.get("host"),
                    Some(Value::Str(s)) if s == "smtp.example.com"
                ));
                assert!(matches!(map.get("port"), Some(Value::Int(587))));
                assert!(matches!(
                    map.get("message_id"),
                    Some(Value::Str(s)) if s == "<msg-123@example.com>"
                ));
                assert!(matches!(map.get("smtp_code"), Some(Value::Int(250))));
                assert!(matches!(
                    map.get("smtp_message"),
                    Some(Value::Str(s)) if s == "2.0.0 queued"
                ));
            }
            other => panic!("resolved value should be dict, got {}", other.repr()),
        }
    }

    #[tokio::test]
    async fn email_send_supports_multipart_attachments() {
        let captured = Arc::new(tokio::sync::Mutex::new(None::<String>));
        let captured_for_sender = captured.clone();

        let email = EmailHandle::with_sender_for_test(
            EmailConfig {
                host: "smtp.example.com".into(),
                port: 587,
                username: None,
                password: None,
                default_from: Some("Relay <noreply@example.com>".into()),
                tls_mode: EmailTlsMode::StartTls,
            },
            Arc::new(move |msg| {
                let captured = captured_for_sender.clone();
                Box::pin(async move {
                    let formatted = String::from_utf8_lossy(&msg.formatted()).to_string();
                    *captured.lock().await = Some(formatted);
                    Ok(EmailSendMetadata::default())
                })
            }),
        );

        let mut attachment = IndexMap::new();
        attachment.insert("filename".into(), Value::Str("hello.txt".into()));
        attachment.insert("content_type".into(), Value::Str("text/plain".into()));
        attachment.insert("bytes".into(), Value::Bytes(b"hello world".to_vec()));

        let send_fn = match email.get_member("send") {
            Value::Builtin(f) => f,
            other => panic!("send should be builtin, got {}", other.repr()),
        };

        let out = send_fn(
            vec![
                Value::Str("ada@example.com".into()),
                Value::Str("Attachment test".into()),
                Value::Str("See attachment".into()),
            ],
            vec![("attachments".into(), Value::List(vec![Value::Dict(attachment)]))],
        )
        .await
        .expect("send should succeed");

        let deferred = match out {
            Value::Deferred(d) => d,
            other => panic!("send should return Deferred, got {}", other.repr()),
        };
        let _ = deferred.resolve().await.expect("send deferred should resolve");

        let formatted = captured
            .lock()
            .await
            .clone()
            .expect("formatted message should be captured");
        assert!(
            formatted.contains("multipart/mixed"),
            "email with attachments should use multipart/mixed"
        );
        assert!(
            formatted.contains("filename=\"hello.txt\""),
            "attachment filename should be present in MIME headers"
        );
    }

    #[tokio::test]
    async fn email_send_requires_body_part() {
        let email = EmailHandle::with_sender_for_test(
            EmailConfig {
                host: "smtp.example.com".into(),
                port: 587,
                username: None,
                password: None,
                default_from: Some("Relay <noreply@example.com>".into()),
                tls_mode: EmailTlsMode::StartTls,
            },
            Arc::new(|_msg| Box::pin(async move { Ok(EmailSendMetadata::default()) })),
        );

        let send_fn = match email.get_member("send") {
            Value::Builtin(f) => f,
            other => panic!("send should be builtin, got {}", other.repr()),
        };

        let err = send_fn(
            vec![
                Value::Str("ada@example.com".into()),
                Value::Str("No body".into()),
            ],
            vec![],
        )
        .await;
        let err = match err {
            Ok(_) => panic!("missing body should fail"),
            Err(err) => err,
        };

        assert!(matches!(
            err,
            RelayError::Type(msg) if msg.contains("requires at least one body part")
        ));
    }

    #[tokio::test]
    async fn email_render_and_render_file_support_template_data() {
        let email = EmailHandle::new(EmailConfig {
            host: "smtp.example.com".into(),
            port: 587,
            username: None,
            password: None,
            default_from: Some("Relay <noreply@example.com>".into()),
            tls_mode: EmailTlsMode::StartTls,
        });

        let mut data = IndexMap::new();
        data.insert("name".into(), Value::Str("Ada".into()));

        let render_fn = match email.get_member("render") {
            Value::Builtin(f) => f,
            other => panic!("render should be builtin, got {}", other.repr()),
        };

        let rendered_inline = render_fn(
            vec![
                Value::Str("Hello {{ name }}".into()),
                Value::Dict(data.clone()),
            ],
            vec![],
        )
        .await
        .expect("render should succeed");
        assert!(matches!(
            rendered_inline,
            Value::Str(s) if s == "Hello Ada"
        ));

        let file_name = format!(
            "relay-email-template-{}.txt",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time should be after epoch")
                .as_nanos()
        );
        let template_path = std::env::temp_dir().join(file_name);
        tokio::fs::write(&template_path, "File says hi {{ name }}")
            .await
            .expect("template write should work");

        let render_file_fn = match email.get_member("render_file") {
            Value::Builtin(f) => f,
            other => panic!("render_file should be builtin, got {}", other.repr()),
        };
        let rendered_file = render_file_fn(
            vec![
                Value::Str(template_path.to_string_lossy().to_string()),
                Value::Dict(data),
            ],
            vec![],
        )
        .await
        .expect("render_file call should succeed");
        let rendered_file = match rendered_file {
            Value::Deferred(d) => d.resolve().await.expect("render_file deferred should resolve"),
            other => panic!("render_file should return Deferred, got {}", other.repr()),
        };
        assert!(matches!(
            rendered_file,
            Value::Str(s) if s == "File says hi Ada"
        ));

        let _ = tokio::fs::remove_file(template_path).await;
    }

    #[tokio::test]
    async fn supports_index_assignment_and_missing_dict_keys() {
        let src = r#"numbers = [1, 2, 3]
numbers[0] = 10
session = {}
session["user"] = "alice"
is_missing_none = (session["missing"] == None)
"#;

        let program = parse_src(src).expect("program should parse");
        let env = Arc::new(tokio::sync::Mutex::new(Env::new_global()));
        let evaluator = Arc::new(Evaluator::new(env.clone()));
        {
            let mut env_lock = env.lock().await;
            install_stdlib(&mut env_lock, evaluator.clone()).expect("stdlib install should work");
        }

        evaluator
            .eval_program(&program)
            .await
            .expect("program should evaluate");

        let env_lock = env.lock().await;

        let numbers = env_lock.get("numbers").expect("numbers should exist");
        match numbers {
            Value::List(values) => {
                assert!(matches!(values.first(), Some(Value::Int(10))));
            }
            _ => panic!("numbers should be a list"),
        }

        let session = env_lock.get("session").expect("session should exist");
        match session {
            Value::Dict(map) => {
                assert!(matches!(map.get("user"), Some(Value::Str(s)) if s == "alice"));
            }
            _ => panic!("session should be a dict"),
        }

        let missing = env_lock
            .get("is_missing_none")
            .expect("is_missing_none should exist");
        assert!(matches!(missing, Value::Bool(true)));
    }
}
