namespace Reiny;

/// <summary>An explicitly owned native handle. Dispose is idempotent.</summary>
public abstract class Resource : IDisposable
{
    private readonly object gate = new();
    private ulong handle;

    internal Resource(ulong handle) => this.handle = handle;

    internal T WithHandle<T>(Func<ulong, T> operation)
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(handle == 0, this);
            return operation(handle);
        }
    }

    public void Dispose()
    {
        lock (gate)
        {
            if (handle == 0) return;
            Native.Check(Native.Release(handle));
            handle = 0;
        }
        GC.SuppressFinalize(this);
    }
}

public sealed class LocalBus : Resource
{
    private LocalBus(ulong handle) : base(handle) { }

    public static LocalBus New()
    {
        Native.Check(Native.LocalBusNew(out var handle));
        return new LocalBus(handle);
    }

    public Session Connect(string id, string domain = "default") => WithHandle(bus =>
    {
        Native.Check(Native.LocalBusConnect(bus, Native.Text(id, nameof(id)),
            Native.Text(domain, nameof(domain)), out var handle));
        return new Session(handle);
    });
}

public sealed class Session : Resource
{
    internal Session(ulong handle) : base(handle) { }

    public static Session Open(string id, string domain = "default", string? zenohConfig = null)
    {
        Native.Check(Native.SessionOpen(Native.Text(id, nameof(id)), Native.Text(domain, nameof(domain)),
            zenohConfig is null ? null : Native.Text(zenohConfig, nameof(zenohConfig)), out var handle));
        return new Session(handle);
    }

    public Publisher Publisher(string topic, ulong? schema = null) => WithHandle(session =>
    {
        Native.Check(Native.SessionPublisher(session, Native.Text(topic, nameof(topic)),
            schema.HasValue ? (byte)1 : (byte)0, schema.GetValueOrDefault(), out var handle));
        return new Publisher(handle);
    });

    public Subscription Subscriber(string topic, string? source = null) => WithHandle(session =>
    {
        Native.Check(Native.SessionSubscriber(session, Native.Text(topic, nameof(topic)),
            source is null ? null : Native.Text(source, nameof(source)), out var handle));
        return new Subscription(handle);
    });

    public IReadOnlyList<string> Publishers(string topic, ulong timeoutMs) => WithHandle(session =>
    {
        Native.Check(Native.SessionPublishers(session, Native.Text(topic, nameof(topic)), timeoutMs, out var list));
        try
        {
            Native.Check(Native.ListLen(list, out var length));
            var result = new List<string>(checked((int)length));
            for (nuint index = 0; index < length; index++)
            {
                Native.Check(Native.ListGet(list, index, out var buffer));
                result.Add(Native.StringBuffer(buffer));
            }
            return (IReadOnlyList<string>)result;
        }
        finally { Native.Check(Native.Release(list)); }
    });

    public void Shutdown() => WithHandle(session =>
    {
        Native.Check(Native.SessionShutdown(session));
        return 0;
    });
}

public sealed class Publisher : Resource
{
    internal Publisher(ulong handle) : base(handle) { }

    public void Send(byte[] payload) => WithHandle(publisher =>
    {
        ArgumentNullException.ThrowIfNull(payload);
        Native.Check(Native.PublisherSend(publisher, payload, (nuint)payload.Length));
        return 0;
    });
}

public sealed record Message(byte[] Payload, string Source, ulong? Schema, ulong? Timestamp);

public sealed class Subscription : Resource
{
    internal Subscription(ulong handle) : base(handle) { }

    /// <summary>Blocks until a message arrives or the timeout expires; null means timeout.</summary>
    public Message? Receive(ulong timeoutMs) => WithHandle<Message?>(subscription =>
    {
        Native.Check(Native.SubscriptionReceive(subscription, timeoutMs, out var message));
        if (message == 0) return null;
        try
        {
            Native.Check(Native.MessagePayload(message, out var payloadHandle));
            var payload = Native.Buffer(payloadHandle);
            Native.Check(Native.MessageSource(message, out var sourceHandle));
            var source = Native.StringBuffer(sourceHandle);
            Native.Check(Native.MessageSchema(message, out var hasSchema, out var schema));
            Native.Check(Native.MessageTimestamp(message, out var hasTimestamp, out var timestamp));
            return new Message(payload, source, hasSchema != 0 ? schema : null,
                hasTimestamp != 0 ? timestamp : null);
        }
        finally { Native.Check(Native.Release(message)); }
    });
}
