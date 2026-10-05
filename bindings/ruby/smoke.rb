# Exercise the generated UniFFI API using Ruby's ffi gem.
require "reiny_ffi"

bus = ReinyFfi::LocalBus.new
sender = bus.connect("ruby-sender", "smoke")
receiver = bus.connect("ruby-receiver", "smoke")
publisher = sender.publisher("Ping", (1 << 64) - 1)
subscriber = receiver.subscriber("Ping", "ruby-sender")
payload = [10, 4, 0, 1, 255, 0].pack("C*")

publisher.send(payload)
message = subscriber.receive(1000)
raise "Missing or changed payload" unless message && message.payload == payload
raise "Wrong source" unless message.source == "ruby-sender"
raise "Wrong schema" unless message.schema == (1 << 64) - 1
raise "Missing publisher" unless receiver.publishers("Ping", 1000) == ["ruby-sender"]
receiver.shutdown
raise "Receive continued after shutdown" unless subscriber.receive(1000).nil?
puts "Ruby UniFFI smoke passed"
