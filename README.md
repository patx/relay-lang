# Relay Programming Language

**Version:** 0.1
**License:** MIT

> A networking-first, async-by-default programming language with Python-like syntax and Node.js-style non-blocking semantics.

Relay is designed for building high-performance web services, APIs, and network applications with minimal boilerplate. Everything is asynchronous by default, but there's **no `await` keyword**—async operations start immediately and only block when their values are actually needed.



## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Language Guide](#language-guide)
  - [Syntax Fundamentals](#syntax-fundamentals)
  - [Data Types](#data-types)
  - [Control Flow](#control-flow)
  - [Functions](#functions)
  - [Async Model](#async-model)
- [API Reference](#api-reference)
  - [Core Functions](#core-functions)
  - [Async & Concurrency](#async--concurrency)
  - [File System](#file-system)
  - [HTTP Client](#http-client)
  - [Web Server](#web-server)
  - [Database (MongoDB)](#database-mongodb)
- [Examples](#examples)
- [How It Works](#how-it-works)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)



## Features

- **Implicit Async**: No `await` keyword needed—async operations resolve automatically when values are accessed
- **Python-like Syntax**: Clean, indentation-based syntax that's easy to read and write
- **Built-in Web Server**: Flask/FastAPI-style decorators with automatic routing
- **Route Groups + OpenAPI**: Prefix route groups and expose generated `/openapi.json`
- **Static File Serving**: Mount directories directly in your web app (`app.static("/assets", "./public")`)
- **Middleware Hooks**: Run request middleware before handlers (`app.use(auth_middleware)`)
- **HTTP Client**: Async HTTP client with `get/post/put/patch/delete` and request/response headers
- **MongoDB Integration**: Native async MongoDB support
- **Session Management**: Built-in session handling with HttpOnly cookies
- **Pluggable Session Backends**: Use in-memory sessions or custom load/save callbacks
- **Authentication Helpers**: Password hashing/verification and pluggable auth stores
- **Structured API Errors**: `HTTPError(...)` helper with consistent JSON error envelope
- **Request IDs**: Per-request `request_id` in handler context and `x-request-id` response header
- **Template Rendering**: MiniJinja-powered templates available in any string expression (`{{ variable }}`)
- **Type Hints**: Optional runtime type checking for function parameters
- **List Comprehensions**: Python-style inline list transforms with optional filtering
- **Destructuring Assignment**: Unpack lists/strings/dicts into multiple variables
- **Error Handling**: `try/except` blocks for graceful runtime error recovery
- **Concurrency Primitives**: `spawn`, `all`, `race`, `timeout`, and `cancel` for parallel execution



## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/relay.git
cd relay

# Install the Relay CLI
cargo install --path .

# Run a Relay program
relay path/to/app.ry
```

### Hello World

```relay
print("Hello, Relay!")
```

### Async Hello World

```relay
sleep(2000, print("world"))
print("hello")
```

**Output:**
```
hello
world
```

Notice how "hello" prints immediately while "world" waits 2 seconds—all without blocking the main thread.

### Simple Web Server

```relay
app = WebApp()
server = WebServer()

fn auth()
    if (request.path == "/private" && session["user"] == None)
        return app.redirect("/login")

app.use(auth)
app.static("/assets", "./public")

@app.get("/")
fn index()
    return "Hello, Relay!"

server.run(app)
```

Visit `http://127.0.0.1:8080/` to see your response.



## Installation

### Prerequisites

- Rust 1.70 or later
- Cargo (included with Rust)

### From Source

```bash
# Clone the repository
git clone https://github.com/yourusername/relay.git
cd relay

# Build and install
cargo install --path .

# Run a Relay program
relay path/to/app.ry
```

### Development Mode

If you're developing the Relay compiler itself:

```bash
cargo run -- path/to/file.ry
```

### Environment Variables

- `RELAY_BIND`: Override the default bind address for web servers (default: `127.0.0.1:8080`)

Example:
```bash
RELAY_BIND=0.0.0.0:8080 relay server.ry
```



## Language Guide

### Syntax Fundamentals

Relay uses **indentation-based syntax** with 4 spaces per indentation level. Tabs are not allowed.

```relay
fn example()
    x = 10
    if (x > 5)
        print("x is greater than 5")
    else
        print("x is 5 or less")
```

**Key Rules:**
- Indentation must be exactly 4 spaces per level
- No tabs allowed
- Function bodies, control flow blocks, and loops all require indentation
- Comments start with `//` and continue to the end of the line

```relay
// This is a comment
x = 42  // This is also a comment
```

### Modules and Imports

Relay supports loading code from multiple `.ry` files with `import`:

```relay
import utils
import web.routes
import shared/helpers.ry
```

**How imports resolve:**
- `import utils` loads `utils.ry`
- `import web.routes` loads `web/routes.ry`
- Relative paths are resolved from the importing file's directory
- A module is loaded only once per run (duplicate imports are ignored)

Imported modules execute in the same global scope, so functions and variables they define become directly available.

### Data Types

#### Primitives

```relay
// Integers
age = 25
count = -10

// Floats
pi = 3.14159
temperature = -273.15

// Strings
name = "Relay"
message = "Hello, world!"

// Booleans
is_active = True
is_complete = False

// None
result = None
```

#### Collections

**Lists:**
```relay
numbers = [1, 2, 3, 4, 5]
mixed = [1, "two", 3.0, True]
nested = [[1, 2], [3, 4]]

// List comprehensions
squares = [n * n for n in numbers]
evens = [n for n in numbers if n % 2 == 0]

// Access elements
first = numbers[0]      // 1
last = numbers[4]       // 5

// Lists are mutable
numbers[0] = 10
```

**Dictionaries:**
```relay
user = {"name": "Ada", "age": 30, "active": True}

// Access values
name = user["name"]     // "Ada"
age = user["age"]       // 30

// Keys are always strings
// Values can be any type
```

**Important:** Dictionary keys are automatically stringified. `{1: "value"}` becomes `{"1": "value"}`.

#### Destructuring Assignment

Relay supports destructuring assignment for iterables such as lists, strings, and dictionaries (dictionary keys are unpacked):

```relay
a, b, c = [10, 20, 30]
x, y = "hi"           // x = "h", y = "i"
k1, k2 = {"a": 1, "b": 2}
```

The number of variables must match the number of unpacked values.

### Control Flow

#### If-Else

```relay
if (condition)
    // then block
    print("condition is true")
```

```relay
if (x > 0)
    print("positive")
```

#### While Loops

```relay
i = 0
while (i < 5)
    print(i)
    i = i + 1
```

Augmented assignment operators are supported:
```relay
i = 0
while (i < 5)
    print(i)
    i =+ 1  // equivalent to i = i + 1
```

#### For Loops

```relay
// Iterate over lists
for (item in [1, 2, 3, 4, 5])
    print(item)

// Iterate over dictionary keys
user = {"name": "Ada", "age": 30}
for (key in user)
    print(key + ": " + str(user[key]))
```

**Note:** For loops iterate over collection elements or dictionary keys.

#### Error Handling (`try/except`)

Use `try/except` to catch runtime errors and continue execution:

```relay
try
    value = int("not-a-number")
    print(value)
except
    print("Could not parse integer")
```

You can also bind the error message:

```relay
try
    result = missing_name + 1
except(err)
    print("Error:", err)
```

### Functions

#### Basic Functions

```relay
fn greet(name)
    return "Hello, " + name

message = greet("World")
print(message)  // Hello, World
```

#### Default Parameters

```relay
fn greet(name: str = "World")
    return "Hello, " + name

print(greet())          // Hello, World
print(greet("Relay"))   // Hello, Relay
```

#### Type Hints

Relay supports runtime type checking for parameters:

```relay
fn add(a: int, b: int)
    return a + b

result = add(5, 10)         // OK
result = add("5", "10")     // Type error!
```

Type hints on regular functions are strict validation (no implicit coercion). Unknown type hints raise a runtime type error.

**Supported Types:**
- `str`: String
- `int`: Integer
- `float`: Float
- `bool`: Boolean
- `json` or `Json`: JSON object

```relay
fn process_data(data: json)
    return data["key"]
```

#### Return Values

Functions can return any value:

```relay
fn get_user()
    return {"name": "Ada", "id": 1}

fn calculate()
    return 42

fn do_work()
    // No explicit return = returns None
    print("Working...")
```

### Async Model

Relay's async model is unique: **there is no `await` keyword**. Instead, async operations return `Deferred` values that automatically resolve when you try to use them.

#### Expression Statements Don't Block

When you call an async function without using its return value, it runs in the background:

```relay
sleep(1000, print("delayed"))
print("immediate")
```

**Output:**
```
immediate
delayed
```

#### Deferred Values

When you assign the result of an async operation, you get a `Deferred` value. The operation starts immediately but doesn't block:

```relay
x = sleep(1000, 10)     // Returns immediately with Deferred<10>
y = sleep(1000, 20)     // Returns immediately with Deferred<20>
print(x + y)            // Waits for both, then prints 30
```

Both sleeps start at the same time, so this takes ~1 second, not 2.

#### How Deferred Resolution Works

A `Deferred` value automatically resolves (waits for the async operation to complete) when:

1. **Used in an expression:**
```relay
result = deferred_value + 10
```

2. **Passed to a function:**
```relay
print(deferred_value)
```

3. **Returned from a function:**
```relay
fn get_data()
    return http.get("https://api.example.com")
```

4. **Used in a comparison:**
```relay
if (deferred_value > 10)
    print("greater than 10")
```

#### Concurrency Primitives

**`spawn(expr)`** - Run an expression in parallel:
```relay
fn work(n)
    sleep(500, n * 2)

task1 = spawn(work(5))
task2 = spawn(work(10))
result1 = task1.join()  // Wait for completion
result2 = task2.join()
```

**`all(tasks)`** - Wait for all tasks to complete:
```relay
tasks = [spawn(work(1)), spawn(work(2)), spawn(work(3))]
results = all(tasks)    // [2, 4, 6]
```

**`race(tasks)`** - Wait for the first task to complete:
```relay
tasks = [spawn(sleep(1000, "slow")), spawn(sleep(100, "fast"))]
winner = race(tasks)    // "fast"
```

**`timeout(expr, ms)`** - Add a timeout to any operation:
```relay
result = timeout(http.get("https://slow-api.com"), 5000)
```

**`cancel(task)`** - Cancel a running task:
```relay
task = spawn(long_operation())
cancel(task)
```



## API Reference

### Core Functions

#### `print(value, ...)`
Print values to stdout. Multiple arguments are printed space-separated.

```relay
print("Hello")                  // Hello
print("x =", 42)                // x = 42
print("a", "b", "c")            // a b c
```

#### `str(value)`
Convert any value to a string.

```relay
str(123)        // "123"
str(3.14)       // "3.14"
str(True)       // "True"
str([1, 2, 3])  // "[1, 2, 3]"
```

#### `int(value)`
Convert a value to an integer.

```relay
int("42")       // 42
int(3.9)        // 3
int(True)       // 1
int("invalid")  // Runtime error
```

#### `float(value)`
Convert a value to a float.

```relay
float("3.14")   // 3.14
float(42)       // 42.0
float("2.5e3")  // 2500.0
```

### Async & Concurrency

#### `sleep(milliseconds, value=None)`
**Returns:** `Deferred<value>`

Sleep for the specified duration, then resolve to `value`.

```relay
sleep(1000, print("done"))          // Print after 1 second
result = sleep(2000, 42)            // Wait 2s, result = 42
```

#### `spawn(expression)`
**Returns:** `Task`

Execute an expression in parallel. Returns a `Task` object.

```relay
task = spawn(expensive_computation())
// Do other work...
result = task.join()
```

**Task Methods:**
- `task.join()` - Wait for the task to complete and return its value

#### `all(tasks)`
**Returns:** List of results

Wait for all tasks to complete. Returns results in order.

```relay
tasks = [spawn(work(1)), spawn(work(2)), spawn(work(3))]
results = all(tasks)  // Wait for all, returns [result1, result2, result3]
```

#### `race(tasks)`
**Returns:** First completed result

Wait for the first task to complete, return its result.

```relay
tasks = [
    spawn(http.get("https://api1.com")),
    spawn(http.get("https://api2.com"))
]
fastest = race(tasks)  // Returns whichever completes first
```

#### `timeout(expression, milliseconds)`
**Returns:** `Deferred<value>` or timeout error

Add a timeout to any async operation.

```relay
result = timeout(http.get("https://slow.com"), 5000)  // 5 second timeout
```

If the timeout is exceeded, a runtime error is raised.

#### `cancel(task)`
Cancel a running task.

```relay
task = spawn(long_running_operation())
cancel(task)
```

### File System

#### `read_file(path)`
**Returns:** `Deferred<string>`

Read a file's contents as a UTF-8 string.

```relay
content = read_file("data.txt")
print(content)
```

#### `save_file(content, path)`
**Returns:** `Deferred<None>`

Write a string to a file.

```relay
save_file("Hello, World!", "output.txt")
```

#### `read_json(path)`
**Returns:** `Deferred<dict>`

Read and parse a JSON file.

```relay
data = read_json("config.json")
print(data["api_key"])
```

#### `save_json(data, path)`
**Returns:** `Deferred<None>`

Serialize data to JSON and write to a file.

```relay
config = {"host": "localhost", "port": 8080}
save_json(config, "config.json")
```

### HTTP Client

#### `Http()`
Create an HTTP client instance.

```relay
http = Http()
```

#### `http.get(url, headers=None)`
**Returns:** `Deferred<Response>`

Send a GET request.

```relay
http = Http()
resp = http.get(
    "https://api.example.com/users",
    headers={"authorization": "Bearer token"}
)
print(resp.status)  // 200
print(resp.text)    // Response body as string
```

#### `http.post(url, data=None, headers=None)`
#### `http.put(url, data=None, headers=None)`
#### `http.patch(url, data=None, headers=None)`
#### `http.delete(url, data=None, headers=None)`
**Returns:** `Deferred<Response>`

Send requests with optional payloads and headers.

```relay
http = Http()
payload = {"name": "Ada", "email": "ada@example.com"}
resp = http.post(
    "https://api.example.com/users",
    payload,
    headers={"content-type": "application/json"}
)
```

#### Response Object

HTTP responses have the following properties:

- `resp.status` - HTTP status code (int)
- `resp.text` - Response body as string
- `resp.json()` - Parse response body as JSON
- `resp.headers` - Response headers (dict)

```relay
http = Http()
resp = http.get("https://api.github.com/users/octocat")

print(resp.status)  // 200
data = resp.json()
print(data["login"])                  // octocat
```

### Web Server

#### `WebApp()`
Create a web application instance.

```relay
app = WebApp()
```

#### Route Decorators

Define HTTP endpoints using decorators:

- `@app.get(path)`
- `@app.post(path)`
- `@app.put(path)`
- `@app.patch(path)`
- `@app.delete(path)`
- `@app.ws(path)`

```relay
app = WebApp()

@app.get("/")
fn index()
    return {"message": "Welcome to Relay"}

@app.post("/users")
fn create_user(name: str, email: str)
    return {"id": 123, "name": name, "email": email}
```

Decorator schema options:
- `validate=...` - Validate/coerce body payload (JSON first, then form)
- `query=...` - Validate/coerce query parameters
- `body=...` - Validate/coerce form body
- `json=...` - Validate/coerce JSON body

```relay
@app.get("/search", query={"limit": "int", "q?": "str"})
fn search(limit, q = None)
    return {"limit": limit, "q": q}

@app.post("/users", json={"name": "str", "age?": "int"})
fn create_user(name, age = None)
    return {"name": name, "age": age}
```

#### Route Groups and OpenAPI

Use grouped route prefixes to organize larger APIs:

```relay
app = WebApp()
api = app.group("/api")
v1 = api.group("/v1")

@v1.get("/users/<user_id>")
fn get_user(user_id)
    return {"id": user_id}
```

Enable generated OpenAPI docs:

```relay
app.openapi(title="Relay API", version="1.0.0")
// Exposes GET /openapi.json
```

#### WebSocket Routes

Use `@app.ws(path)` for WebSocket upgrade endpoints.
WebSocket handlers receive a `socket` object with:
- `socket.recv()` → returns text (`str`), binary (`bytes`), or `None` on close
- `socket.send(value)` → sends text (or binary when `bytes`)
- `socket.close()` → closes the connection

```relay
@app.ws("/chat")
fn chat_room()
    while True
        msg = socket.recv()
        if (msg == None)
            break
        socket.send("echo: " + str(msg))
```

#### Path Parameters

Use `<name>` syntax to capture path segments:

```relay
@app.get("/users/<user_id>")
fn get_user(user_id)
    return {"id": user_id, "name": "Ada"}

@app.get("/posts/<post_id>/comments/<comment_id>")
fn get_comment(post_id, comment_id)
    return {"post": post_id, "comment": comment_id}
```

#### Handler Parameters

Handler parameters are automatically bound from:
1. **Path parameters** (highest priority)
2. **Request body form fields** (`application/x-www-form-urlencoded`)
3. **Query parameters** (lowest priority)

```relay
// GET /search?q=relay&limit=10
@app.get("/search")
fn search(q: str, limit: int = 20)
    return {"query": q, "limit": limit}

// POST /users with form body: name=Ada&email=ada@example.com
@app.post("/users")
fn create_user(name: str, email: str)
    return {"name": name, "email": email}

// GET /users/123
@app.get("/users/<user_id>")
fn get_user(user_id)
    return {"id": user_id}
```

JSON bodies are available as the `data` parameter (default name) or by typing a handler param as `Json`.

```relay
// POST /events with JSON body {"type":"signup","user":"ada"}
@app.post("/events")
fn create_event(data: Json)
    return {"event_type": data["type"], "user": data["user"]}
```

For zero-arg handlers (or explicit access), use request helpers: `get_query()`, `get_body()`, and `get_json()`.

#### Middleware

Register middleware with `app.use(fn)`.

Middleware functions can be:
- `fn middleware()` (legacy style)
- `fn middleware(ctx)` where `ctx` contains request fields
- `fn middleware(ctx, next)` with full chain semantics

If middleware returns a non-`None` value, Relay short-circuits and sends that response.

`next()` runs the remainder of the middleware chain and then the handler.

```relay
fn audit(ctx, next)
    print("before:", ctx["path"])
    result = next()
    print("after:", ctx["path"])
    return result
```

#### Type Hints in Handlers

Use type hints to enforce parameter types and enable automatic coercion:

```relay
@app.post("/calculate")
fn calculate(a: int, b: int)
    return {"result": a + b}

// POST /calculate with form body a=5&b=10
// Automatically converts strings to ints: {"result": 15}
```

**Supported types:**
- `str` - String
- `int` - Integer
- `float` - Float
- `bool` - Boolean
- `json` or `Json` - Full JSON body (for POST/PUT/PATCH)

#### Request Object

Every handler has access to a `request` dictionary:

```relay
@app.get("/debug")
fn debug_request()
    print(request["method"])    // GET
    print(request["path"])      // /debug
    print(request["request_id"])// rid_...
    print(request["query"])     // Query parameters dict
    print(request["form"])      // Form fields dict (if present)
    print(request["json"])      // JSON body (if present)
    print(request["headers"])   // Headers dict
    print(request["cookies"])   // Cookies dict
    return "OK"
```

**Request fields:**
- `method` - HTTP method (string)
- `path` - Request path (string)
- `request_id` - Request identifier string
- `query` - Query parameters (dict)
- `form` - Parsed form body fields (dict, when present)
- `headers` - Request headers (dict)
- `cookies` - Cookies (dict)
- `json` - Parsed JSON body (if present)

#### Request Helpers

Use helpers when you want payload access without binding handler parameters:

- `get_query()` - Query parameters as a dict (empty dict when unavailable)
- `get_body()` - Parsed form fields as a dict (empty dict when unavailable)
- `get_json()` - Parsed JSON body (or `None` when unavailable)

```relay
@app.get("/search")
fn search()
    query = get_query()
    return {"q": query["q"]}

@app.post("/submit")
fn submit()
    form = get_body()
    payload = get_json()
    return {"form": form, "json": payload}
```

#### Validation Helpers

Use schema helpers for explicit request validation:

- `validate(data, schema)` - Validate/coerce an object against schema
- `require_query(schema)` - Validate query params in handlers
- `require_body(schema)` - Validate form body in handlers
- `require_json(schema)` - Validate JSON body in handlers

Schema format:
- `"field": "type"` for required fields
- `"field?": "type"` for optional fields
- `"field": {"type": "int", "required": True, "default": 10}` for explicit rules

```relay
@app.get("/search")
fn search()
    params = require_query({"limit": "int", "q?": "str"})
    return {"limit": params["limit"], "q": params["q"]}
```

#### Cookies

Access cookies via the `cookies` dict:

```relay
@app.get("/")
fn index()
    user_id = cookies["user_id"]
    return "User ID: " + user_id
```

#### Sessions

Relay provides built-in session management with HttpOnly cookies:

```relay
@app.get("/login")
fn login(username: str)
    session["user"] = username
    session["logged_in"] = True
    return "Logged in"

@app.get("/profile")
fn profile()
    if (session["logged_in"] == True)
        return "Welcome, " + session["user"]
    else
        return app.redirect("/login")
```

**Session features:**
- Automatically persisted across requests
- Stored server-side (not in cookies)
- Uses `relay_sid` cookie (`HttpOnly`, `SameSite=Lax`, `Secure` automatically on HTTPS)
- Session data is a dictionary that persists modifications

Customize cookie policy:

```relay
app.session(secure=True, http_only=True, same_site="Lax")
```

Use a custom session backend (for any database/service):

```relay
session_db = {}

fn load_session(sid)
    return session_db[sid]

fn save_session(sid, data)
    session_db[sid] = data

app.session_backend(load_session, save_session)
```

#### Response Types

Handlers can return various types:

**JSON (automatic):**
```relay
@app.get("/api/user")
fn get_user()
    return {"name": "Ada", "id": 123}  // Auto-serialized to JSON
```

**Plain text:**
```relay
@app.get("/")
fn index()
    return "Hello, World!"  // Content-Type: text/plain
```

**HTML:**
```relay
@app.get("/")
fn index()
    return "<h1>Welcome</h1>"  // Content-Type: text/html
```

**Custom Response:**
```relay
@app.get("/custom")
fn custom()
    return Response(
        {"error": "Not found"},
        status=404,
        content_type="application/json"
    )
```

**Redirect:**
```relay
@app.post("/old-path")
fn old_endpoint()
    return app.redirect("/new-path")
```

#### `Response(body, status=200, content_type=None)`
Create a custom HTTP response.

```relay
@app.get("/xml")
fn get_xml()
    xml = "<root><item>data</item></root>"
    return Response(xml, status=200, content_type="application/xml")
```

**Parameters:**
- `body` - Response body (string, dict, list, or bytes)
- `status` - HTTP status code (default: 200)
- `content_type` - Content-Type header (auto-detected if not specified)

#### `HTTPError(status, code, message, details=None)`
Create a structured API error response.
When called inside a handler, Relay also includes `request_id` in the error payload.

```relay
@app.post("/users")
fn create_user(name)
    if (name == None)
        return HTTPError(400, "validation_error", "Missing name", {"field": "name"})
    return {"ok": True}
```

#### Authentication Helpers

#### `auth_hash_password(password)`
**Returns:** password hash string

#### `auth_verify_password(password, hash)`
**Returns:** `bool`

```relay
hash = auth_hash_password("super-secret")
is_valid = auth_verify_password("super-secret", hash)   // True
```

#### `AuthStore(load_fn=None, save_fn=None)`
Create an authentication store.

- `AuthStore()` uses in-memory storage.
- `AuthStore(load_fn, save_fn)` uses custom callbacks for pluggable backends.

Available methods:
- `store.register(username, password)` - hashes and stores password
- `store.verify(username, password)` - verifies against stored hash
- `store.get_hash(username)` - returns stored hash or `None`
- `store.set_hash(username, hash)` - stores precomputed hash

```relay
store = AuthStore()
store.register("ada", "pw")
print(store.verify("ada", "pw"))  // True

auth_db = {}
fn load_user(name)
    return auth_db[name]

fn save_user(name, hash)
    auth_db[name] = hash

custom = AuthStore(load_user, save_user)
custom.register("bob", "pw2")
print(custom.verify("bob", "pw2"))  // True
```

#### `app.redirect(url)`
**Returns:** `Response` with 302 status

Create a redirect response.

```relay
@app.post("/submit")
fn submit(data)
    // Process data...
    return app.redirect("/success")
```

#### Template Rendering (MiniJinja)

Relay uses **MiniJinja** (Rust implementation of Jinja2) for template interpolation in strings.

Template strings are evaluated anywhere in the interpreter (not only in web handlers) when a string contains both `{{` and `}}`.

```relay
name = "Relay"
version = "0.1"

title = "{{ name }} v{{ version }}"
print(title)  // Relay v0.1

items = ["a", "b", "c"]
print("Count: {{ items | length }}")
```

Web handlers use the same engine:

```relay
@app.get("/")
fn index()
    return "<h1>{{ name }} v{{ version }}</h1>"
```

Templates can reference values in the current scope and support MiniJinja expressions/filters.

#### `WebServer()`
Create a web server instance.

```relay
server = WebServer()
```

#### `server.run(app)`
Start the web server.

```relay
app = WebApp()
server = WebServer()

@app.get("/")
fn index()
    return "Hello, World!"

server.run(app)  // Starts server on 127.0.0.1:8080
```

**Configuration:**
- Default bind address: `127.0.0.1:8080`
- Override with `RELAY_BIND` environment variable:
  ```bash
  RELAY_BIND=0.0.0.0:8080 relay server.ry
  ```

### Database (MongoDB)

#### `Mongo(connection_string)`
Create a MongoDB client.

```relay
mongo = Mongo("mongodb://localhost:27017")
```

**Connection string format:**
```
mongodb://[username:password@]host[:port][/database]
```

Examples:
```relay
// Local MongoDB
mongo = Mongo("mongodb://localhost:27017")

// MongoDB Atlas
mongo = Mongo("mongodb+srv://user:pass@cluster.mongodb.net/")

// With authentication
mongo = Mongo("mongodb://admin:password@localhost:27017")
```

#### `mongo.db(database_name)`
**Returns:** Database instance

Access a database.

```relay
mongo = Mongo("mongodb://localhost:27017")
db = mongo.db("my_app")
```

#### `db.collection(collection_name)`
**Returns:** Collection instance

Access a collection.

```relay
users = db.collection("users")
posts = db.collection("posts")
```

#### `collection.insert_one(document)`
**Returns:** `Deferred<dict>` with `inserted_id`

Insert a single document.

```relay
users = db.collection("users")
result = users.insert_one({"name": "Ada", "email": "ada@example.com"})
print(result["inserted_id"])  // ObjectId as string
```

#### `collection.insert_many(documents)`
**Returns:** `Deferred<dict>` with `inserted_ids`

Insert multiple documents.

```relay
users = db.collection("users")
docs = [
    {"name": "Ada", "email": "ada@example.com"},
    {"name": "Grace", "email": "grace@example.com"}
]
result = users.insert_many(docs)
print(result["inserted_ids"])  // Dict of index -> ObjectId string
```

#### `collection.find_one(filter)`
**Returns:** `Deferred<dict>` or `None`

Find a single document matching the filter.

```relay
users = db.collection("users")
user = users.find_one({"email": "ada@example.com"})
if (user != None)
    print(user["name"])
```

**Filter examples:**
```relay
// Exact match
user = users.find_one({"name": "Ada"})

// Multiple conditions (implicit AND)
user = users.find_one({"name": "Ada", "active": True})

// By ObjectId
user = users.find_one({"_id": "507f1f77bcf86cd799439011"})
```

#### `collection.find(filter)`
**Returns:** `Deferred<list>` of documents

Find all documents matching the filter.

```relay
users = db.collection("users")
active_users = users.find({"active": True})
for (user in active_users)
    print(user["name"])
```

**Find all documents:**
```relay
all_users = users.find({})
```

#### `collection.update_one(filter, update)`
**Returns:** `Deferred<dict>` with `matched_count` and `modified_count`

Update a single document.

```relay
users = db.collection("users")
result = users.update_one(
    {"email": "ada@example.com"},
    {"$set": {"active": True}}
)
print(result["modified_count"])  // 1
```

**Update operators:**
```relay
// Set fields
users.update_one({"_id": id}, {"$set": {"status": "active"}})

// Increment
users.update_one({"_id": id}, {"$inc": {"login_count": 1}})

// Unset fields
users.update_one({"_id": id}, {"$unset": {"temp_field": ""}})
```

#### `collection.update_many(filter, update)`
**Returns:** `Deferred<dict>` with `matched_count` and `modified_count`

Update multiple documents.

```relay
users = db.collection("users")
result = users.update_many(
    {"active": False},
    {"$set": {"status": "inactive"}}
)
print(result["modified_count"])
```

#### `collection.delete_one(filter)`
**Returns:** `Deferred<dict>` with `deleted_count`

Delete a single document.

```relay
users = db.collection("users")
result = users.delete_one({"email": "ada@example.com"})
print(result["deleted_count"])  // 1 or 0
```

#### `collection.delete_many(filter)`
**Returns:** `Deferred<dict>` with `deleted_count`

Delete multiple documents.

```relay
users = db.collection("users")
result = users.delete_many({"active": False})
print(result["deleted_count"])  // Number of deleted documents
```



## Examples

### 1. Hello World (Async)

```relay
sleep(2000, print("world"))
print("hello")
```

**Output:**
```
hello
world
```

### 2. Simple Web API

```relay
app = WebApp()
server = WebServer()

@app.get("/health")
fn health()
    return {"status": "ok", "service": "relay-api"}

@app.get("/users/<user_id>")
fn get_user(user_id)
    return {"id": user_id, "name": "Ada Lovelace"}

server.run(app)
```

### 3. Pastebin Service

```relay
app = WebApp()
server = WebServer()
mongo = Mongo("mongodb://localhost:27017")
db = mongo.db("pastebin")
pastes = db.collection("pastes")

fn find_paste(paste_id)
    return pastes.find_one({"_id": paste_id})

@app.get("/")
fn index()
    return read_file("static/index.html")

@app.post("/")
fn create_paste(content = None)
    if (content == None)
        return Response("Missing content", status=400)
    result = pastes.insert_one({"content": content})
    paste_id = str(result["inserted_id"])
    return app.redirect("/" + paste_id)

@app.get("/<paste_id>")
fn view_paste(paste_id)
    paste = find_paste(paste_id)
    if (paste == None)
        return Response("Not found", status=404)
    return read_file("static/paste.html")

@app.get("/api/paste/<paste_id>")
fn get_paste(paste_id)
    paste = find_paste(paste_id)
    if (paste == None)
        return Response("Not found", status=404)
    return {"id": paste_id, "content": "{{ paste[\"content\"] }}"}

server.run(app)
```

Template files used by this example:
- `static/index.html` for paste creation form
- `static/paste.html` for `<pre><code>` viewer UI and syntax highlighting

### 4. Concurrent HTTP Requests

```relay
http = Http()

fn fetch_user(user_id)
    resp = http.get("https://api.example.com/users/" + str(user_id))
    return resp.json()

// Fetch 5 users concurrently
tasks = []
i = 1
while (i <= 5)
    tasks =+ [spawn(fetch_user(i))]
    i =+ 1

users = all(tasks)
for (user in users)
    print(user["name"])
```

### 5. File Processing Pipeline

```relay
fn process_file(filename)
    content = read_file(filename)
    lines = len(content.split("\n"))
    return {"file": filename, "lines": lines}

files = ["data1.txt", "data2.txt", "data3.txt"]
tasks = []
for (f in files)
    tasks =+ [spawn(process_file(f))]

results = all(tasks)
save_json(results, "report.json")
print("Processing complete!")
```

### 6. Session-based Authentication

```relay
app = WebApp()
server = WebServer()
mongo = Mongo("mongodb://localhost:27017")
db = mongo.db("auth_demo")
users = db.collection("users")

@app.get("/")
fn index()
    if (session["authenticated"] == True)
        return "Welcome, " + session["username"]
    return app.redirect("/login")

@app.post("/login")
fn login(username: str, password: str)
    user = users.find_one({"username": username})
    if (user == None)
        return Response("Invalid credentials", status=401)
    
    // In production, use proper password hashing!
    if (user["password"] == password)
        session["authenticated"] = True
        session["username"] = username
        return app.redirect("/")
    
    return Response("Invalid credentials", status=401)

@app.get("/logout")
fn logout()
    session["authenticated"] = False
    session["username"] = None
    return app.redirect("/login")

server.run(app)
```

### 7. REST API with MongoDB

```relay
app = WebApp()
server = WebServer()
mongo = Mongo("mongodb://localhost:27017")
db = mongo.db("blog")
posts = db.collection("posts")

@app.get("/posts")
fn list_posts()
    all_posts = posts.find({})
    return all_posts

@app.post("/posts")
fn create_post(title: str, content: str, author: str)
    result = posts.insert_one({
        "title": title,
        "content": content,
        "author": author
    })
    return {"id": str(result["inserted_id"])}

@app.get("/posts/<post_id>")
fn get_post(post_id)
    post = posts.find_one({"_id": post_id})
    if (post == None)
        return Response("Post not found", status=404)
    return post

@app.put("/posts/<post_id>")
fn update_post(post_id, title: str, content: str)
    result = posts.update_one(
        {"_id": post_id},
        {"$set": {"title": title, "content": content}}
    )
    if (result["matched_count"] == 0)
        return Response("Post not found", status=404)
    return {"updated": True}

@app.delete("/posts/<post_id>")
fn delete_post(post_id)
    result = posts.delete_one({"_id": post_id})
    if (result["deleted_count"] == 0)
        return Response("Post not found", status=404)
    return {"deleted": True}

server.run(app)
```

### 8. Timeout and Error Handling

```relay
http = Http()

fn fetch_with_timeout(url)
    return timeout(http.get(url), 5000)

// Try to fetch with 5 second timeout
result = fetch_with_timeout("https://slow-api.com/data")
print(result.text)
```

### 9. Race Condition Example

```relay
http = Http()

// Fetch from multiple mirrors, use whichever responds first
mirrors = [
    "https://mirror1.example.com/data",
    "https://mirror2.example.com/data",
    "https://mirror3.example.com/data"
]

tasks = []
for (url in mirrors)
    tasks =+ [spawn(http.get(url))]

fastest = race(tasks)
print("Fastest mirror returned:", fastest.text)
```

### 10. Background Task Processing

```relay
fn process_item(item)
    sleep(1000, print("Processed: " + str(item)))

items = [1, 2, 3, 4, 5]

// Spawn all tasks without waiting
for (item in items)
    spawn(process_item(item))

print("All tasks started, continuing...")
// Tasks run in background
```



## How It Works

### Architecture Overview

Relay is built on:
- **Rust**: The interpreter is written in Rust for performance and safety
- **Tokio**: Async runtime for non-blocking I/O
- **Axum**: High-performance web framework for the built-in server
- **MongoDB driver**: Native async MongoDB support

### Compilation Pipeline

1. **Lexer**: Tokenizes source code with indentation-aware parsing
2. **Parser**: Builds an Abstract Syntax Tree (AST)
3. **Evaluator**: Interprets the AST with async/await translation

### The Async Model in Detail

#### Deferred Values

When you call an async function, Relay immediately starts the operation and returns a `Deferred` value:

```relay
// This starts the HTTP request immediately
response = http.get("https://api.example.com")
// response is Deferred<Response>

// The request is already in-flight here
print("Request started")

// Only when we access response.status does it wait
print(response.status)  // <-- Blocks here if not complete
```

#### Auto-Resolution

`Deferred` values automatically resolve when:

1. **Used in operations:**
```relay
x = sleep(1000, 10)
y = x + 5  // Waits for x to resolve
```

2. **Passed to functions:**
```relay
result = sleep(1000, 42)
print(result)  // Waits before printing
```

3. **Used in control flow:**
```relay
data = http.get("https://api.example.com")
if (data.status == 200)  // Waits before comparison
    print("Success")
```

4. **Indexed:**
```relay
resp = http.get("https://api.example.com")
json_data = resp.json()
print(json_data["key"])  // Waits for json() before indexing
```

#### Expression Statements

Expression statements (expressions not assigned to variables) run without blocking:

```relay
// This starts the sleep but doesn't wait
sleep(1000, print("delayed"))

// This prints immediately
print("immediate")

// Output:
// immediate
// delayed (after 1 second)
```

### Concurrency Model

Relay uses Tokio's work-stealing scheduler to run tasks concurrently:

```relay
// Start 3 HTTP requests concurrently
task1 = spawn(http.get("https://api1.com"))
task2 = spawn(http.get("https://api2.com"))
task3 = spawn(http.get("https://api3.com"))

// Wait for all to complete
results = all([task1, task2, task3])
```

All three requests run in parallel, completing in the time of the slowest request (not 3× the time).

### Web Server Architecture

The web server uses Axum's routing system:

1. **Route Registration**: Decorators like `@app.get("/path")` register handlers
2. **Request Handling**: Incoming requests are matched against registered routes
3. **Parameter Binding**: Path/query/body parameters are extracted and bound to handler parameters
4. **Type Coercion**: Type hints trigger automatic type conversion
5. **Response Generation**: Return values are automatically serialized to appropriate content types

### Session Storage

Sessions are stored server-side in an in-memory hash map:
- Session ID is generated by Relay at runtime
- `relay_sid` cookie stores the session ID (`HttpOnly`, `SameSite=Lax`, `Secure` on HTTPS)
- Session data persists across requests for the same session ID
- Sessions are stored in memory (cleared on server restart)

Every web request is also assigned a `request_id` and echoed as the `x-request-id` response header.

**Note:** In production, you'd want to persist sessions to a database.

### MongoDB Integration

MongoDB operations return `Deferred` values that resolve when the database operation completes:

```relay
// This starts the query immediately
users = collection.find({"active": True})

// The query is running in the background here
print("Query started")

// Only when we iterate do we wait for results
for (user in users)  // <-- Blocks here
    print(user["name"])
```



## Best Practices

### 1. Leverage Concurrent Execution

Instead of:
```relay
// Sequential (slow)
result1 = http.get("https://api1.com")
result2 = http.get("https://api2.com")
result3 = http.get("https://api3.com")
```

Do:
```relay
// Concurrent (fast)
tasks = [
    spawn(http.get("https://api1.com")),
    spawn(http.get("https://api2.com")),
    spawn(http.get("https://api3.com"))
]
results = all(tasks)
```

### 2. Use Type Hints for API Handlers

Type hints provide automatic validation and coercion:

```relay
@app.post("/calculate")
fn calculate(a: int, b: int, operation: str = "add")
    if (operation == "add")
        return {"result": a + b}
    else
        return {"result": a - b}
```

### 3. Handle Missing Data Gracefully

Always check for `None` when querying databases or processing optional parameters:

```relay
@app.get("/users/<user_id>")
fn get_user(user_id)
    user = users.find_one({"_id": user_id})
    if (user == None)
        return Response("User not found", status=404)
    return user
```

### 4. Use Sessions for State Management

Don't try to maintain state in global variables. Use sessions:

```relay
// Bad
current_user = None

@app.post("/login")
fn login(username)
    current_user = username  // Won't work across requests

// Good
@app.post("/login")
fn login(username)
    session["user"] = username
```

### 5. Implement Timeouts for External Calls

Always add timeouts to external HTTP requests:

```relay
fn fetch_data(url)
    return timeout(http.get(url), 10000)  // 10 second timeout
```

### 6. Structure Large Applications

Split handlers into logical groups:

```relay
app = WebApp()
server = WebServer()

// Auth routes
@app.post("/auth/login")
fn login(username, password)
    // ...

@app.post("/auth/logout")
fn logout()
    // ...

// User routes
@app.get("/users/<user_id>")
fn get_user(user_id)
    // ...

@app.post("/users")
fn create_user(name, email)
    // ...

// Post routes
@app.get("/posts")
fn list_posts()
    // ...

server.run(app)
```

### 7. Use Augmented Assignment

For cleaner counter increments:

```relay
// Instead of
i = i + 1

// Use
i =+ 1
```

### 8. Return Early for Error Cases

Structure handlers with early returns for error cases:

```relay
@app.get("/posts/<post_id>")
fn get_post(post_id)
    post = posts.find_one({"_id": post_id})
    if (post == None)
        return Response("Not found", status=404)
    
    if (post["published"] == False)
        return Response("Not published", status=403)
    
    return post
```



## Troubleshooting

### Common Errors

#### "Indentation must be 4 spaces per level"

**Cause:** Relay requires exactly 4 spaces per indentation level.

**Fix:** Ensure all indentation uses 4 spaces (not tabs, not 2 spaces).

```relay
// Wrong
fn example()
  print("hello")  // 2 spaces

// Right
fn example()
    print("hello")  // 4 spaces
```

#### "Tabs are not allowed (spaces only)"

**Cause:** Relay does not support tabs for indentation.

**Fix:** Configure your editor to use spaces instead of tabs.

#### "Type error: Cannot add int and str"

**Cause:** Attempting to use incompatible types in an operation.

**Fix:** Use explicit type conversion:

```relay
// Wrong
x = 10 + "5"

// Right
x = 10 + int("5")
```

#### "Name error: Undefined variable 'x'"

**Cause:** Using a variable before it's defined.

**Fix:** Ensure variables are assigned before use:

```relay
// Wrong
print(x)
x = 10

// Right
x = 10
print(x)
```

#### "Runtime error: Index out of bounds"

**Cause:** Accessing a list index that doesn't exist.

**Fix:** Check list length before accessing:

```relay
items = [1, 2, 3]
if (len(items) > 5)
    print(items[5])
```

### Debugging Tips

1. **Use print statements:** Relay's simplest debugging tool
```relay
fn process_data(data)
    print("Processing:", data)  // Debug output
    result = transform(data)
    print("Result:", result)   // Debug output
    return result
```

2. **Check async resolution:** If something seems to hang, check if you're waiting for a `Deferred` value
```relay
// This might hang if the HTTP request never completes
result = http.get("https://unreachable.com")
print(result.status)

// Add a timeout:
result = timeout(http.get("https://unreachable.com"), 5000)
```

3. **Verify MongoDB connection:** Test your connection string in the MongoDB shell first

4. **Check file paths:** File operations use paths relative to where you run the `relay` command

5. **Inspect request objects:** Log the request object to debug handler issues
```relay
@app.post("/debug")
fn debug()
    print(request)
    return "OK"
```

### Performance Tips

1. **Batch database operations:** Use `insert_many` instead of multiple `insert_one` calls
```relay
// Slow
for (item in items)
    collection.insert_one(item)

// Fast
collection.insert_many(items)
```

2. **Use `spawn` for I/O-heavy tasks:** Parallelize independent operations
```relay
// Serial: 5 seconds total
sleep(1000, "a")
sleep(1000, "b")
sleep(1000, "c")
sleep(1000, "d")
sleep(1000, "e")

// Parallel: 1 second total
all([
    spawn(sleep(1000, "a")),
    spawn(sleep(1000, "b")),
    spawn(sleep(1000, "c")),
    spawn(sleep(1000, "d")),
    spawn(sleep(1000, "e"))
])
```

3. **Minimize synchronous operations:** Keep handlers fast to avoid blocking the event loop

## Contributing

Contributions are welcome! Here's how to get started:

### Development Setup

```bash
# Clone the repo
git clone https://github.com/yourusername/relay.git
cd relay

# Build in debug mode
cargo build

# Run tests
cargo test

# Run the sample app
cargo run -- test.ry
```

### Adding Features

1. **Lexer changes:** Modify the `Lexer` struct and `tokenize()` method
2. **Parser changes:** Update the `Parser` struct and AST types
3. **Runtime changes:** Modify the `Evaluator` and `install_stdlib()` function
4. **Testing:** Add runnable `.ry` scripts and corresponding docs snippets

### Coding Standards

- Follow Rust conventions and `rustfmt` formatting
- Add comments for complex logic
- Core interpreter remains in a single Rust file (v0.1) while Relay scripts support multi-file imports
- Update this README for any user-facing changes

### Reporting Issues

Found a bug? [Open an issue](https://github.com/patx/relay-lang/issues) with:
- Relay version
- Operating system
- Minimal reproducible example
- Expected vs. actual behavior


## Roadmap

**v0.2 (Planned):**
- [x] Multiple file support and imports
- [x] List comprehensions
- [x] Destructuring assignment
- [x] Error handling with try/except
- [x] WebSocket support
- [X] Static file serving
- [X] Middleware support

**v0.3 (Future):**
- [ ] Package manager
- [ ] Standard library expansion
- [ ] SQL database support (PostgreSQL, SQLite)
- [ ] Redis integration
- [ ] Interactive REPL with syntax highlighting
- [ ] Web framework improvements (routing groups, validation)
- [ ] Worker processes for CPU-heavy tasks

**Known Gaps (Identified in v0.1):**
- [ ] JSON request key binding to scalar handler args (e.g. bind `{"name":"Ada"}` directly to `fn create(name)`).
- [x] HTTP client parity for `put`, `patch`, `delete`, request headers, and response header access.
- [ ] First-class CLI flags (`--help`, `--version`) for better install verification and discoverability.
- [ ] Built-in HTML escaping helper for safely rendering user content directly in server-side templates.


## License

MIT License

Copyright (c) 2026 Harrison Erd

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


## Support

- **Documentation:** This README
- **Reference app:** `test.ry` with templates under `static/`
- **Issues:** [GitHub Issues](https://github.com/patx/relay-lang/issues)
- **Discussions:** [GitHub Discussions](https://github.com/patx/relay-lang/discussions)
