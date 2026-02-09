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

## Core Concepts

### Async by default

```relay
text = read_file("hello.txt")
print(text)
```

### Expression statements never block

```relay
sleep(2000, "world")
print("hello")
```

Output:
```
hello
world
```

### Implicit async resolution

```relay
x = sleep(1000, 10)
y = sleep(1000, 20)

print(x + y)
```

---

## Functions

```relay
fn slow(x)
    sleep(1000)
    return x * 2

print(slow(21))
```

---

## Web Server

```relay
app = WebApp()

@app.get("/")
fn index()
    return "Hello world"

@app.get("/<name>")
fn hello(name)
    return "Hello {{ name }}"

WebServer.run(app)
```

---

## License

MIT
