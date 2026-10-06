import assert from "node:assert/strict";
import { LocalBus } from "../index.js";

const bus = LocalBus.new();
const owned = [bus];
const keep = (resource) => (owned.push(resource), resource);
try {
  const sender = keep(bus.connect("group/sender", "demo"));
  const receiver = keep(bus.connect("receiver", "demo"));
  const other = keep(bus.connect("group/other", "demo"));
  const isolated = keep(bus.connect("isolated", "other-domain"));
  const all = keep(receiver.subscriber("Binary"));
  const filtered = keep(receiver.subscriber("Binary", "group/sender"));
  const domainFiltered = keep(isolated.subscriber("Binary"));
  const schema = 18446744073709551615n;
  const publisher = keep(sender.publisher("Binary", schema));
  const otherPublisher = keep(other.publisher("Binary"));
  assert.deepEqual(receiver.publishers("Binary", 1000).sort(), ["group/other", "group/sender"]);
  const payload = new Uint8Array([0, 1, 0, 255]);
  publisher.send(payload);
  const message = all.receive(1000);
  assert.ok(message);
  assert.deepEqual(message.payload, payload);
  assert.equal(message.source, "group/sender");
  assert.equal(message.schema, schema);
  assert.ok(message.timestamp === undefined || typeof message.timestamp === "bigint");
  assert.deepEqual(filtered.receive(1000)?.payload, payload);
  otherPublisher.send(new Uint8Array([2]));
  assert.equal(all.receive(1000)?.source, "group/other");
  // Matching sentinels follow the rejected samples on the ordered local bus.
  publisher.send(Uint8Array.of(3));
  assert.deepEqual(filtered.receive(1000)?.payload, Uint8Array.of(3));
  const isolatedPublisher = keep(isolated.publisher("Binary"));
  isolatedPublisher.send(Uint8Array.of(4));
  assert.deepEqual(domainFiltered.receive(1000)?.payload, Uint8Array.of(4));
  assert.throws(() => sender.publisher(""));
  assert.throws(() => bus.connect("bad//id", "demo"));
  assert.throws(() => publisher.send("not binary"));
  assert.throws(() => sender.publisher("Overflow", 18446744073709551616n));
  publisher.dispose();
  assert.throws(() => publisher.send(payload));
  receiver.shutdown();
  console.log("JavaScript local pub/sub smoke passed");
} finally {
  for (const resource of owned.reverse()) resource.dispose();
}
