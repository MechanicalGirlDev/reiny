import Foundation
import Reiny

let bus = LocalBus()
let sender = try bus.connect(id: "swift-sender", domain: "smoke")
let receiver = try bus.connect(id: "swift-receiver", domain: "smoke")
let publisher = try sender.publisher(topic: "Ping", schema: UInt64.max)
let subscriber = try receiver.subscriber(topic: "Ping", source: "swift-sender")
let payload = Data([10, 4, 0, 1, 255, 0])

try publisher.send(payload: payload)
guard let message = try subscriber.receive(timeoutMs: 1000) else {
    fatalError("No message received")
}
precondition(message.payload == payload)
precondition(message.source == "swift-sender")
precondition(message.schema == UInt64.max)
let publishers = try receiver.publishers(topic: "Ping", timeoutMs: 1000)
precondition(publishers == ["swift-sender"])
receiver.shutdown()
let afterShutdown = try subscriber.receive(timeoutMs: 1000)
precondition(afterShutdown == nil)
print("Swift UniFFI smoke passed")
