import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
const fixture = (name: string) => fileURLToPath(new URL(`../../fixtures/${name}.json`, import.meta.url));
import { LocalBus, type Message } from "../index.js";

const bus = LocalBus.new();
const resources: Array<{ dispose(): void }> = [bus];
function keep<T extends { dispose(): void }>(resource: T): T {
  resources.push(resource);
  return resource;
}
try {
  const sender = keep(bus.connect("sender", "typescript", fixture("sender")));
  const receiver = keep(bus.connect("receiver", "typescript", fixture("receiver")));
  const subscription = keep(receiver.subscriber("in", "Binary", 42n));
  const schema: bigint = 42n;
  const publisher = keep(sender.publisher("out", "Binary", schema));
  sender.ready();
  receiver.ready();
  const payload = Uint8Array.of(0, 1, 0, 255);
  publisher.send(payload);
  const message: Message | undefined = subscription.receive(1000);
  assert.ok(message);
  assert.deepEqual(message.payload, payload);
  assert.equal(message.source, "sender");
  assert.equal(message.schema, schema);
  console.log("TypeScript local pub/sub smoke passed");
} finally {
  for (const resource of resources.reverse()) resource.dispose();
}
