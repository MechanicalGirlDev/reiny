using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;

namespace Reiny;

internal static class Native
{
    private const string Library = "reiny_ffi";

    static Native()
    {
        NativeLibrary.SetDllImportResolver(typeof(Native).Assembly, Resolve);
    }

    private static IntPtr Resolve(string name, Assembly assembly, DllImportSearchPath? searchPath)
    {
        var path = Environment.GetEnvironmentVariable("REINY_FFI_LIBRARY");
        return name == Library && !string.IsNullOrEmpty(path)
            ? NativeLibrary.Load(path) : IntPtr.Zero;
    }

    internal static void Check(int status)
    {
        if (status != 0)
            throw new InvalidOperationException(Marshal.PtrToStringUTF8(LastError()) ?? "reiny native error");
    }

    internal static string Text(string value, string name)
    {
        ArgumentNullException.ThrowIfNull(value, name);
        if (value.Contains('\0')) throw new ArgumentException("Strings cannot contain NUL characters", name);
        return value;
    }

    internal static byte[] Buffer(ulong handle)
    {
        try
        {
            Check(BufferLen(handle, out var length));
            Check(BufferData(handle, out var pointer));
            var data = new byte[checked((int)length)];
            if (data.Length != 0) Marshal.Copy(pointer, data, 0, data.Length);
            return data;
        }
        finally { Check(Release(handle)); }
    }

    internal static string StringBuffer(ulong handle) => Encoding.UTF8.GetString(Buffer(handle));

    [DllImport(Library, EntryPoint = "reiny_last_error", CallingConvention = CallingConvention.Cdecl)]
    private static extern IntPtr LastError();
    [DllImport(Library, EntryPoint = "reiny_release", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int Release(ulong handle);
    [DllImport(Library, EntryPoint = "reiny_local_bus_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int LocalBusNew(out ulong handle);
    [DllImport(Library, EntryPoint = "reiny_local_bus_connect", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int LocalBusConnect(ulong bus, [MarshalAs(UnmanagedType.LPUTF8Str)] string id, [MarshalAs(UnmanagedType.LPUTF8Str)] string domain, out ulong handle);
    [DllImport(Library, EntryPoint = "reiny_session_open", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int SessionOpen([MarshalAs(UnmanagedType.LPUTF8Str)] string id, [MarshalAs(UnmanagedType.LPUTF8Str)] string domain, [MarshalAs(UnmanagedType.LPUTF8Str)] string? config, out ulong handle);
    [DllImport(Library, EntryPoint = "reiny_session_publisher", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int SessionPublisher(ulong session, [MarshalAs(UnmanagedType.LPUTF8Str)] string topic, byte hasSchema, ulong schema, out ulong handle);
    [DllImport(Library, EntryPoint = "reiny_session_subscriber", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int SessionSubscriber(ulong session, [MarshalAs(UnmanagedType.LPUTF8Str)] string topic, [MarshalAs(UnmanagedType.LPUTF8Str)] string? source, out ulong handle);
    [DllImport(Library, EntryPoint = "reiny_session_publishers", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int SessionPublishers(ulong session, [MarshalAs(UnmanagedType.LPUTF8Str)] string topic, ulong timeout, out ulong handle);
    [DllImport(Library, EntryPoint = "reiny_session_shutdown", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int SessionShutdown(ulong session);
    [DllImport(Library, EntryPoint = "reiny_publisher_send", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int PublisherSend(ulong publisher, byte[] data, nuint size);
    [DllImport(Library, EntryPoint = "reiny_subscription_receive", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int SubscriptionReceive(ulong subscription, ulong timeout, out ulong handle);
    [DllImport(Library, EntryPoint = "reiny_message_payload", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int MessagePayload(ulong message, out ulong handle);
    [DllImport(Library, EntryPoint = "reiny_message_source", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int MessageSource(ulong message, out ulong handle);
    [DllImport(Library, EntryPoint = "reiny_message_schema", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int MessageSchema(ulong message, out byte has, out ulong value);
    [DllImport(Library, EntryPoint = "reiny_message_timestamp", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int MessageTimestamp(ulong message, out byte has, out ulong value);
    [DllImport(Library, EntryPoint = "reiny_list_len", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ListLen(ulong list, out nuint length);
    [DllImport(Library, EntryPoint = "reiny_list_get", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ListGet(ulong list, nuint index, out ulong handle);
    [DllImport(Library, EntryPoint = "reiny_buffer_len", CallingConvention = CallingConvention.Cdecl)]
    private static extern int BufferLen(ulong buffer, out nuint length);
    [DllImport(Library, EntryPoint = "reiny_buffer_data", CallingConvention = CallingConvention.Cdecl)]
    private static extern int BufferData(ulong buffer, out IntPtr pointer);
}
