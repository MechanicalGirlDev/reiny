# Exercise the generated UniFFI API using Ruby's ffi gem.
require "reiny_ffi"

fixtures = File.expand_path("../fixtures", __dir__)
bus = ReinyFfi::LocalBus.new
sender = bus.connect("sender", "smoke", File.join(fixtures, "sender.json"))
receiver = bus.connect("receiver", "smoke", File.join(fixtures, "receiver.json"))
publisher = sender.publisher("out", "Binary", 42)
subscriber = receiver.subscriber("in", "Binary", 42)
sender.ready
receiver.ready
payload = [10, 4, 0, 1, 255, 0].pack("C*")

publisher.send(payload)
message = subscriber.receive(1000)
raise "Missing or changed payload" unless message && message.payload == payload
raise "Wrong source" unless message.source == "sender"
raise "Wrong schema" unless message.schema == 42
receiver.shutdown
raise "Receive continued after shutdown" unless subscriber.receive(1000).nil?
puts "Ruby UniFFI smoke passed"
