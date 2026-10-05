import assert from "node:assert/strict";
import { LocalBus, type Message } from "../index.js";

const bus = LocalBus.new();
const resources: Array<{ dispose(): void }> = [bus];
function keep<T extends { dispose(): void }>(resource: T): T {
  resources.push(resource);
  return resource;
}
try {
  const sender = keep(bus.connect("typescript-sender", "typescript"));
  const receiver = keep(bus.connect("typescript-receiver", "typescript"));
  const subscription = keep(receiver.subscriber("Binary", "typescript-sender"));
  const schema: bigint = 18446744073709551615n;
  const publisher = keep(sender.publisher("Binary", schema));
  const payload = Uint8Array.of(0, 1, 0, 255);
  publisher.send(payload);
  const message: Message | undefined = subscription.receive(1000);
  assert.ok(message);
  assert.deepEqual(message.payload, payload);
  assert.equal(message.source, "typescript-sender");
  assert.equal(message.schema, schema);
  assert.deepEqual(receiver.publishers("Binary", 1000), ["typescript-sender"]);
  console.log("TypeScript local pub/sub smoke passed");
} finally {
  for (const resource of resources.reverse()) resource.dispose();
}
