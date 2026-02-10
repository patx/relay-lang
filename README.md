# Relay v0.1

**Relay** is a networking-first, async-by-default programming language with a simple, Python-like syntax and Node-style non-blocking semantics.

There is **no `await` keyword**.
Async work starts immediately and only blocks **when a value is actually needed**.

> Everything is async, but nothing waits unless it has to.

---

## Why Relay?

Most languages make async:
- verbose (`await` everywhere),
- fragile (easy to accidentally block),
- or bolted on later.

Relay flips the model:

- IO is **always non-blocking**
- Async values resolve **implicitly**
- Concurrency is the default, not an opt-in
- Networking and web servers are built in

Relay feels like:
- Python syntax
- Node.js runtime behavior
- Rust-level correctness under the hood

---

## Quick start

Install the CLI locally and run a `.ry` file:

```bash
cargo install --path .
relay path/to/file.ry
```

If you're iterating on the compiler, you can still run it directly:

```bash
cargo run -- path/to/file.ry
```

### Project layout

Relay is currently a single-file interpreter:

- `src/main.rs` implements the lexer, parser, evaluator, stdlib, and runtime.
- `examples/` can hold `.ry` samples (see below).

### CLI usage

```bash
relay path/to/app.ry
```

If you run a web server, you can override the bind address:

```bash
RELAY_BIND=0.0.0.0:8080 relay path/to/server.ry
```

Hello world with non-blocking order:

```relay
sleep(2000, print("world"))
print("hello")
```

Output:

```
hello
world
```

---

## Language tour

### Program structure

Relay is indentation-based (4 spaces per level) and uses expression statements:

```relay
fn main()
    print("hello")
    print("world")
```

### Values, collections, and operators

```relay
name = "Relay"
version = 1
pi = 3.1415
is_async = True

nums = [1, 2, 3]
user = {"name": "Ada", "id": 42}

print(name + " v" + str(version))
print(nums[0])
print(user["name"])
```

Operators: `+ - * /`, comparisons (`== != < <= > >=`), and `not` for unary negation.

### Control flow

```relay
if (version >= 1)
    print("stable")
else
    print("experimental")

sum = 0
for (n in [1, 2, 3])
    sum = sum + n

i = 0
while (i < 3)
    print(i)
    i = i + 1
```

### Functions, defaults, and type hints

```relay
fn greet(name: str = "world")
    return "hello " + name

print(greet())
print(greet("relay"))
```

Type hints are enforced at runtime for `str`, `int`, `float`, and `json`/`Json`.

### Dictionaries and JSON

Relay dicts use stringified keys and can be serialized as JSON:

```relay
profile = {"name": "Ada", "lang": "Relay"}
save_json(profile, "profile.json")
loaded = read_json("profile.json")
print(loaded["name"])
```

### Error handling

Relay surfaces runtime errors with a message and location for syntax issues. Some examples:

- Type errors: invalid operators on mismatched types.
- Name errors: use of undefined variables.
- Runtime errors: invalid IO or HTTP failures.

---

## Async model (no `await`)

### Expression statements don't block

```relay
sleep(1000, "done")
print("started")
```

### Deferred values resolve when needed

```relay
x = sleep(1000, 10)
y = sleep(1000, 20)
print(x + y)
```

### Tasks and concurrency

```relay
fn work(n)
    sleep(500, n * 2)

jobs = [spawn(work(2)), spawn(work(5))]
results = all(jobs)
print(results)
```

Other concurrency helpers:
- `timeout(expr, ms)`
- `race([..])`
- `cancel(task)`
- `task.join()` and `deferred.resolve()`

### Tasks vs Deferred values

- `sleep`, IO, and HTTP calls return `Deferred` values.
- `spawn(expr)` returns a `Task`, which can be `join()`-ed.
- Expression statements run without blocking: any returned `Deferred` or `Task` keeps running.

### Built-ins and type coercion

Relay provides lightweight coercions via built-ins:

```relay
print(str(123))
print(int("42"))
print(float(3))
```

---

## Files + JSON

```relay
text = read_file("notes.txt")
print(text)

save_file("hello", "out.txt")

data = {"name": "Relay", "tags": ["async", "io"]}
save_json(data, "data.json")
parsed = read_json("data.json")
print(parsed["name"])
```

---

## HTTP client

```relay
http = Http()
resp = http.get("https://example.com")
print(resp.status)
print(resp.text)

api = http.post("https://httpbin.org/post", {"hello": "relay"})
json = api.json()
print(json["json"]["hello"])
```

`resp.json()` parses the response body to JSON.

---

## MongoDB client

```relay
mongo = Mongo("mongodb://localhost:27017")
db = mongo.db("relay_demo")
users = db.collection("users")

users.insert_one({"name": "Ada", "active": True})
user = users.find_one({"name": "Ada"})
print(user)
```

MongoDB helpers return `Deferred` values, so they run asynchronously until the result is needed.

---

## Web server

```relay
app = WebApp()
server = WebServer()

@app.get("/")
fn index()
    return read_file("docs/index.html")

@app.post("/")
fn create_paste(paste)
    id = "abc123"
    return app.redirect("/" + id)

server.run(app)
```

Notes:
- Flask/FastAPI-like decorators: `@app.get`, `@app.post`, `@app.put`, `@app.patch`, `@app.delete`.
- Path params use `<name>` syntax, e.g. `@app.get("/p/<pid>")`.
- Handler params are auto-bound from query/body/path (path wins over body, body wins over query).
- `app.redirect(url)` returns an HTTP 302 response with a `Location` header.
- `request`, `cookies`, and `session` are injected into each handler scope.
- Returning dict/list/json produces JSON; strings become text/html/plain automatically.
- `session` persists via an HttpOnly `relay_sid` cookie.

### Custom responses

```relay
@app.get("/status")
fn status()
    return Response({"ok": True}, status=201, content_type="application/json")
```

Relay will infer `content_type` when possible; use `Response(...)` to force status codes/content types.

### Pastebin-style example (Mongo + redirect)

```relay
app = WebApp()
server = WebServer()
mongo = Mongo("mongodb://localhost:27017")
db = mongo.db("relay_demo")
pastes = db.collection("pastes")

@app.get("/")
fn index()
    return read_file("docs/index.html")

@app.get("/<pid>")
fn view_paste(pid)
    paste = pastes.find_one({"_id": pid})
    if (paste == None)
        return Response("not found", status=404)
    return str(paste["paste_content"])

@app.post("/")
fn new_paste(paste)
    inserted = pastes.insert_one({"paste_content": paste})
    pid = str(inserted["inserted_id"])
    return app.redirect("/" + pid)

server.run(app)
```

---

## Example apps

### 1) Basic JSON API

```relay
app = WebApp()
server = WebServer()

@app.get("/health")
fn health()
    return {"ok": True, "service": "relay"}

server.run(app)
```

### 2) Echo service with path params and query

```relay
app = WebApp()
server = WebServer()

@app.get("/echo/<name>")
fn echo(name, greeting: str = "hello")
    return greeting + " " + name

server.run(app)
```

### 3) Background work with concurrency

```relay
fn work(n)
    sleep(500, n * 2)

jobs = [spawn(work(2)), spawn(work(5)), spawn(work(7))]
result = all(jobs)
print(result)
```

### 4) HTTP client + JSON processing

```relay
http = Http()
resp = http.get("https://httpbin.org/json")
data = resp.json()
print(data["slideshow"])
```

### 5) Static HTML response with templating

```relay
app = WebApp()
server = WebServer()
title = "Relay"

@app.get("/")
fn index()
    return "<h1>{{ title }}</h1>"

server.run(app)
```

---

## Standard library at a glance

**Core**: `print`, `str`, `int`, `float`

**Async + concurrency**: `sleep`, `timeout`, `spawn`, `cancel`, `all`, `race`

**Files + JSON**: `read_file`, `save_file`, `read_json`, `save_json`

**Web**: `WebApp`, `WebServer`, `Response`, `Http`

**Database**: `Mongo`

---

## License

MIT
