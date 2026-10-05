import { test } from "node:test";
import assert from "node:assert/strict";
import { LocalBus, Session, Publisher, Subscription } from "../index.js";

test("native handles cannot be constructed by callers", () => {
  for (const Type of [LocalBus, Session, Publisher, Subscription])
    assert.throws(() => new Type(), /factory/);
});

test("network string validation runs before loading the library", () => {
  assert.throws(() => Session.open("bad\0id"), TypeError);
  assert.throws(() => Session.open("id", "bad\0domain"), TypeError);
  assert.throws(() => Session.open("id", "domain", "bad\0config"), TypeError);
});
