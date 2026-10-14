import Foundation
import Reiny

let fixtures = URL(fileURLWithPath: CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "bindings/fixtures").standardizedFileURL
func fixture(_ name: String) -> String { fixtures.appendingPathComponent(name + ".json").path }
let bus = LocalBus()
let sender = try bus.connect(id: "sender", domain: "smoke", moduleBindingsPath: fixture("sender"))
let receiver = try bus.connect(id: "receiver", domain: "smoke", moduleBindingsPath: fixture("receiver"))
let publisher = try sender.publisher(name: "out", typeName: "Binary", schema: 42)
let subscriber = try receiver.subscriber(name: "in", typeName: "Binary", schema: 42)
try sender.ready()
try receiver.ready()
let payload = Data([10, 4, 0, 1, 255, 0])

try publisher.send(payload: payload)
guard let message = try subscriber.receive(timeoutMs: 1000) else {
    fatalError("No message received")
}
precondition(message.payload == payload)
precondition(message.source == "sender")
precondition(message.sourcePort == "out")
precondition(message.schema == 42)
receiver.shutdown()
let afterShutdown = try subscriber.receive(timeoutMs: 1000)
precondition(afterShutdown == nil)
print("Swift UniFFI smoke passed")
