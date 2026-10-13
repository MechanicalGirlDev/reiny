import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
const fixture = (name) => fileURLToPath(new URL(`../../fixtures/${name}.json`, import.meta.url));
import { LocalBus } from "../index.js";

const bus = LocalBus.new();
const owned = [bus];
const keep = (resource) => (owned.push(resource), resource);
try {
  const sender = keep(bus.connect("sender", "demo", fixture("sender")));
  const receiver = keep(bus.connect("receiver", "demo", fixture("receiver-multi")));
  const other = keep(bus.connect("other", "demo", fixture("other")));
  const isolated = keep(bus.connect("isolated", "other-domain", fixture("isolated")));
  const all = keep(receiver.subscriber("all", "Binary", 42n));
  const filtered = keep(receiver.subscriber("filtered", "Binary", 42n));
  const domainFiltered = keep(isolated.subscriber("in", "Binary", 42n));
  const schema = 42n;
  const publisher = keep(sender.publisher("out", "Binary", schema));
  const otherPublisher = keep(other.publisher("out", "Binary", 42n));
  sender.ready();
  receiver.ready();
  other.ready();
  const payload = new Uint8Array([0, 1, 0, 255]);
  publisher.send(payload);
  const message = all.receive(1000);
  assert.ok(message);
  assert.deepEqual(message.payload, payload);
  assert.equal(message.source, "sender");
  assert.equal(message.schema, schema);
  assert.ok(message.timestamp === undefined || typeof message.timestamp === "bigint");
  assert.deepEqual(filtered.receive(1000)?.payload, payload);
  otherPublisher.send(new Uint8Array([2]));
  assert.equal(all.receive(1000)?.source, "other");
  // Matching sentinels follow the rejected samples on the ordered local bus.
  publisher.send(Uint8Array.of(3));
  assert.deepEqual(filtered.receive(1000)?.payload, Uint8Array.of(3));
  const isolatedPublisher = keep(isolated.publisher("out", "Binary", 42n));
  isolated.ready();
  isolatedPublisher.send(Uint8Array.of(4));
  assert.deepEqual(domainFiltered.receive(1000)?.payload, Uint8Array.of(4));
  assert.throws(() => sender.publisher("", "Binary", 42n));
  assert.throws(() => bus.connect("bad//id", "demo", fixture("sender")));
  assert.throws(() => publisher.send("not binary"));
  assert.throws(() => sender.publisher("out", "Binary", 18446744073709551616n));
  publisher.dispose();
  assert.throws(() => publisher.send(payload));
  receiver.shutdown();
  console.log("JavaScript local pub/sub smoke passed");
} finally {
  for (const resource of owned.reverse()) resource.dispose();
}
