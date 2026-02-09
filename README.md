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

Build the CLI and run a `.ry` file:

```bash
cargo run -- path/to/file.ry
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

## Web server

```relay
app = WebApp()
route = app.route()
server = WebServer()

@route.get("/")
fn index()
    return "Hello world"

@route.get("/hello/<name>")
fn hello(name)
    return "Hello {{ name }}"

server.run(app)
```

Notes:
- Define `app`/`route` before the decorated handlers.
- Decorators can use `@app.get("/path")` directly instead of `@route.get`.
- Returning a dict/list/json produces a JSON response.

### Custom responses

```relay
@route.get("/status")
fn status()
    return Response({"ok": True}, status=201, content_type="application/json")
```

Relay will infer `content_type` when possible; use `Response(...)` to force status codes or headers.

---

## Standard library at a glance

**Core**: `print`, `str`, `int`, `float`

**Async + concurrency**: `sleep`, `timeout`, `spawn`, `cancel`, `all`, `race`

**Files + JSON**: `read_file`, `save_file`, `read_json`, `save_json`

**Web**: `WebApp`, `WebServer`, `Response`, `Http`

---

## License

MIT
