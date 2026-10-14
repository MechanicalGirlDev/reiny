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

var fixtures = Path.GetFullPath(args.Length == 0 ? "../fixtures" : args[0]);
string Fixture(string name) => Path.Combine(fixtures, name + ".json");

using var bus = LocalBus.New();
using var sender = bus.Connect("sender", "csharp", Fixture("sender"));
using var receiver = bus.Connect("receiver", "csharp", Fixture("receiver-multi"));
using var other = bus.Connect("other", "csharp", Fixture("other"));
using var isolated = bus.Connect("isolated", "other-domain", Fixture("isolated"));
using var all = receiver.Subscriber("all", "Binary", 42);
using var filtered = receiver.Subscriber("filtered", "Binary", 42);
using var domainFiltered = isolated.Subscriber("in", "Binary", 42);
using var publisher = sender.Publisher("out", "Binary", 42UL);
using var otherPublisher = other.Publisher("out", "Binary", 42);
sender.Ready();
receiver.Ready();
other.Ready();
byte[] payload = [0, 1, 0, 255];
publisher.Send(payload);
var message = all.Receive(1000) ?? throw new Exception("Missing message");
Require(message.Payload.SequenceEqual(payload), "Binary payload");
Require(message.Source == "sender" && message.Schema == 42UL, "Provenance and schema");
Require(message.SourcePort == "out", "Named source port");
Require(filtered.Receive(1000)?.Payload.SequenceEqual(payload) == true, "Source filter delivery");
otherPublisher.Send([2]);
Require(all.Receive(1000)?.Source == "other", "Second session delivery");
// Matching sentinels follow rejected samples on the ordered local dispatcher.
publisher.Send([3]);
Require(filtered.Receive(1000)?.Payload.SequenceEqual(new byte[] { 3 }) == true, "Source isolation");
using var isolatedPublisher = isolated.Publisher("out", "Binary", 42);
isolated.Ready();
isolatedPublisher.Send([4]);
Require(domainFiltered.Receive(1000)?.Payload.SequenceEqual(new byte[] { 4 }) == true, "Domain isolation");
Throws(() => sender.Publisher("", "Binary", 42));
Throws(() => bus.Connect("bad//id", "csharp", Fixture("sender")));
Throws(() => sender.Publisher("bad\0port", "Binary", 42));
publisher.Dispose();
try { publisher.Send(payload); throw new Exception("Disposed publisher accepted send"); }
catch (ObjectDisposedException) { }
receiver.Shutdown();
Console.WriteLine("C# local pub/sub smoke passed");
