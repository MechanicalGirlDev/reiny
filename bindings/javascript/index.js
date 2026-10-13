import koffi from "koffi";

let api;
const token = Symbol("native construction");
const maxU64 = (1n << 64n) - 1n;

export class ReinyError extends Error {
  name = "ReinyError";
  constructor(message, status) {
    super(message);
    this.status = status;
  }
}

export function loadLibrary(path = process.env.REINY_FFI_LIBRARY) {
  if (api) throw new Error("The native library is already loaded");
  const name = process.platform === "win32" ? "reiny_ffi.dll"
    : process.platform === "darwin" ? "libreiny_ffi.dylib" : "libreiny_ffi.so";
  const library = koffi.load(path ?? name);
  const signatures = {
    last_error: "const char *reiny_last_error(void)",
    release: "int32_t reiny_release(uint64_t handle)",
    local_bus_new: "int32_t reiny_local_bus_new(_Out_ uint64_t *out)",
    local_bus_connect: "int32_t reiny_local_bus_connect(uint64_t bus, const char *id, const char *domain, const char *module_bindings_path, _Out_ uint64_t *out)",
    session_open: "int32_t reiny_session_open(const char *id, const char *domain, const char *config, const char *module_bindings_path, _Out_ uint64_t *out)",
    session_publisher: "int32_t reiny_session_publisher(uint64_t session, const char *name, const char *type_name, uint64_t schema, _Out_ uint64_t *out)",
    session_subscriber: "int32_t reiny_session_subscriber(uint64_t session, const char *name, const char *type_name, uint64_t schema, _Out_ uint64_t *out)",
    session_ready: "int32_t reiny_session_ready(uint64_t session)",
    session_shutdown: "int32_t reiny_session_shutdown(uint64_t session)",
    publisher_send: "int32_t reiny_publisher_send(uint64_t publisher, const uint8_t *data, size_t size)",
    subscription_receive: "int32_t reiny_subscription_receive(uint64_t subscription, uint64_t timeout, _Out_ uint64_t *out)",
    message_payload: "int32_t reiny_message_payload(uint64_t message, _Out_ uint64_t *out)",
    message_source: "int32_t reiny_message_source(uint64_t message, _Out_ uint64_t *out)",
    message_schema: "int32_t reiny_message_schema(uint64_t message, _Out_ uint8_t *has, _Out_ uint64_t *value)",
    message_timestamp: "int32_t reiny_message_timestamp(uint64_t message, _Out_ uint8_t *has, _Out_ uint64_t *value)",
    buffer_len: "int32_t reiny_buffer_len(uint64_t buffer, _Out_ size_t *out)",
    buffer_data: "int32_t reiny_buffer_data(uint64_t buffer, _Out_ const uint8_t **out)",
  };
  api = Object.fromEntries(Object.entries(signatures).map(([key, signature]) => [key, library.func(signature)]));
}

function native() {
  if (!api) loadLibrary();
  return api;
}

function check(status) {
  if (status !== 0) throw new ReinyError(native().last_error() ?? "reiny native error", status);
}

function output(name, ...args) {
  const out = [0];
  check(native()[name](...args, out));
  return out[0];
}

function text(value, name) {
  if (typeof value !== "string" || value.includes("\0"))
    throw new TypeError(`${name} must be a string without NUL characters`);
  return value;
}

function timeout(value) {
  if (!Number.isSafeInteger(value) || value < 0)
    throw new RangeError("timeoutMs must be a nonnegative safe integer");
  return BigInt(value);
}

function schemaValue(value) {
  if (typeof value !== "bigint" || value < 0n || value > maxU64)
    throw new RangeError("schema must be a bigint in the uint64 range");
  return value;
}

function buffer(handle) {
  try {
    const length = Number(output("buffer_len", handle));
    const pointer = output("buffer_data", handle);
    return length === 0 ? new Uint8Array() : Uint8Array.from(koffi.decode(pointer, "uint8_t", length));
  } finally {
    check(native().release(handle));
  }
}

function optionalValue(name, handle) {
  const has = [0], value = [0];
  check(native()[name](handle, has, value));
  return has[0] ? BigInt(value[0]) : undefined;
}

class Resource {
  #handle;
  constructor(key, handle) {
    if (key !== token) throw new TypeError("Use the documented factory method");
    this.#handle = BigInt(handle);
  }
  get handle() {
    if (this.#handle === 0n) throw new Error("Resource is disposed");
    return this.#handle;
  }
  dispose() {
    if (this.#handle === 0n) return;
    check(native().release(this.#handle));
    this.#handle = 0n;
  }
}

export class LocalBus extends Resource {
  static new() {
    return new LocalBus(token, output("local_bus_new"));
  }
  connect(id, domain, moduleBindingsPath) {
    return new Session(token, output("local_bus_connect", this.handle, text(id, "id"), text(domain, "domain"), text(moduleBindingsPath, "moduleBindingsPath")));
  }
}

export class Session extends Resource {
  static open(id, domain, zenohConfig, moduleBindingsPath) {
    return new Session(token, output("session_open", text(id, "id"), text(domain, "domain"),
      zenohConfig === undefined ? null : text(zenohConfig, "zenohConfig"), text(moduleBindingsPath, "moduleBindingsPath")));
  }
  publisher(name, typeName, schema) {
    return new Publisher(token, output("session_publisher", this.handle, text(name, "name"),
      text(typeName, "typeName"), schemaValue(schema)));
  }
  subscriber(name, typeName, schema) {
    return new Subscription(token, output("session_subscriber", this.handle, text(name, "name"),
      text(typeName, "typeName"), schemaValue(schema)));
  }
  ready() {
    check(native().session_ready(this.handle));
  }
  shutdown() {
    check(native().session_shutdown(this.handle));
  }
}

export class Publisher extends Resource {
  send(payload) {
    const handle = this.handle;
    if (!(payload instanceof Uint8Array)) throw new TypeError("payload must be a Uint8Array");
    check(native().publisher_send(handle, payload, payload.byteLength));
  }
}

export class Subscription extends Resource {
  receive(timeoutMs) {
    const message = BigInt(output("subscription_receive", this.handle, timeout(timeoutMs)));
    if (message === 0n) return undefined;
    try {
      return {
        payload: buffer(output("message_payload", message)),
        source: new TextDecoder().decode(buffer(output("message_source", message))),
        schema: optionalValue("message_schema", message),
        timestamp: optionalValue("message_timestamp", message),
      };
    } finally {
      check(native().release(message));
    }
  }
}
