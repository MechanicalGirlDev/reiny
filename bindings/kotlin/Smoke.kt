import dev.mechanicalgirl.reiny.LocalBus
import java.io.File

/** Run with the generated Reiny bindings and JNA on the classpath. */
fun main(args: Array<String>) {
    val fixtures = File(args.firstOrNull() ?: "bindings/fixtures").absoluteFile
    fun fixture(name: String) = File(fixtures, "$name.json").path
    LocalBus().use { bus ->
        bus.connect("sender", "smoke", fixture("sender")).use { sender ->
            bus.connect("receiver", "smoke", fixture("receiver")).use { receiver ->
                sender.publisher("out", "Binary", 42uL).use { publisher ->
                    receiver.subscriber("in", "Binary", 42uL).use { subscriber ->
                        sender.ready()
                        receiver.ready()
                        val payload = byteArrayOf(10, 4, 0, 1, -1, 0)
                        publisher.send(payload)
                        val message = checkNotNull(subscriber.receive(1000uL))
                        check(message.payload.contentEquals(payload))
                        check(message.source == "sender")
                        check(message.schema == 42uL)
                        receiver.shutdown()
                        check(subscriber.receive(1000uL) == null)
                    }
                }
            }
        }
    }
    println("Kotlin UniFFI smoke passed")
}
