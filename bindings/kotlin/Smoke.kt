import dev.mechanicalgirl.reiny.LocalBus

/** Run with the generated Reiny bindings and JNA on the classpath. */
fun main() {
    LocalBus().use { bus ->
        bus.connect("kotlin-sender", "smoke").use { sender ->
            bus.connect("kotlin-receiver", "smoke").use { receiver ->
                sender.publisher("Ping", ULong.MAX_VALUE).use { publisher ->
                    receiver.subscriber("Ping", "kotlin-sender").use { subscriber ->
                        val payload = byteArrayOf(10, 4, 0, 1, -1, 0)
                        publisher.send(payload)
                        val message = checkNotNull(subscriber.receive(1000uL))
                        check(message.payload.contentEquals(payload))
                        check(message.source == "kotlin-sender")
                        check(message.schema == ULong.MAX_VALUE)
                        check(receiver.publishers("Ping", 1000uL) == listOf("kotlin-sender"))
                        receiver.shutdown()
                        check(subscriber.receive(1000uL) == null)
                    }
                }
            }
        }
    }
    println("Kotlin UniFFI smoke passed")
}
