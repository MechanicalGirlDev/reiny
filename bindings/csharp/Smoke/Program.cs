using Reiny;

static void Require(bool condition, string description)
{
    if (!condition) throw new Exception(description);
}

static void Throws(Action action)
{
    try { action(); }
    catch (InvalidOperationException) { return; }
    catch (ArgumentException) { return; }
    throw new Exception("Expected an error");
}

using var bus = LocalBus.New();
using var sender = bus.Connect("sender", "csharp");
using var receiver = bus.Connect("receiver", "csharp");
using var other = bus.Connect("other", "csharp");
using var isolated = bus.Connect("isolated", "other-domain");
using var all = receiver.Subscriber("Binary");
using var filtered = receiver.Subscriber("Binary", "sender");
using var domainFiltered = isolated.Subscriber("Binary");
using var publisher = sender.Publisher("Binary", ulong.MaxValue);
using var otherPublisher = other.Publisher("Binary");
Require(receiver.Publishers("Binary", 1000).Order().SequenceEqual(new[] { "other", "sender" }), "Discovery");
byte[] payload = [0, 1, 0, 255];
publisher.Send(payload);
var message = all.Receive(1000) ?? throw new Exception("Missing message");
Require(message.Payload.SequenceEqual(payload), "Binary payload");
Require(message.Source == "sender" && message.Schema == ulong.MaxValue, "Provenance and schema");
Require(filtered.Receive(1000)?.Payload.SequenceEqual(payload) == true, "Source filter delivery");
otherPublisher.Send([2]);
Require(all.Receive(1000)?.Source == "other", "Second session delivery");
// Matching sentinels follow rejected samples on the ordered local dispatcher.
publisher.Send([3]);
Require(filtered.Receive(1000)?.Payload.SequenceEqual(new byte[] { 3 }) == true, "Source isolation");
using var isolatedPublisher = isolated.Publisher("Binary");
isolatedPublisher.Send([4]);
Require(domainFiltered.Receive(1000)?.Payload.SequenceEqual(new byte[] { 4 }) == true, "Domain isolation");
Throws(() => sender.Publisher(""));
Throws(() => bus.Connect("bad/id", "csharp"));
Throws(() => sender.Publisher("bad\0topic"));
publisher.Dispose();
try { publisher.Send(payload); throw new Exception("Disposed publisher accepted send"); }
catch (ObjectDisposedException) { }
receiver.Shutdown();
Console.WriteLine("C# local pub/sub smoke passed");
