# C++17 bridge

`crates/reiny-ffi/include/reiny.hpp` wraps the ergonomic C ABI with move-only
RAII handles, `std::optional` receive results and metadata, binary vectors, and
exceptions containing the facade's error text. Include it and link the same
shared library as C. `Session::open` supports the network backend and optional
Zenoh configuration; `LocalBus::connect` shares an in-process bus.

```sh
c++ -std=c++17 -Wall -Wextra -Werror -Icrates/reiny-ffi/include bindings/cpp/smoke.cpp \
  -Ltarget/debug -lreiny_ffi -o target/debug/cpp-smoke
LD_LIBRARY_PATH=target/debug target/debug/cpp-smoke "$PWD/bindings/fixtures/sender.json" "$PWD/bindings/fixtures/receiver.json"
```

On macOS use `DYLD_LIBRARY_PATH=target/debug`. With an initialized Windows
MSVC developer prompt:

```bat
cl /nologo /EHsc /std:c++17 /W4 /WX /Icrates\reiny-ffi\include bindings\cpp\smoke.cpp /Fetarget\debug\cpp-smoke.exe /Fotarget\debug\cpp-smoke.obj /link target\debug\reiny_ffi.dll.lib
target\debug\cpp-smoke.exe "%CD%\bindings\fixtures\sender.json" "%CD%\bindings\fixtures\receiver.json"
```

Each successful raw handle passed to a wrapper constructor transfers ownership
to that wrapper. Do not wrap a handle twice. `get()` only borrows a raw handle;
do not release it manually. Buffer copies and message getter results remain
valid independently of the message. Do not concurrently destroy a wrapper
while another thread uses it.

Both session constructors require an absolute prepared module-bindings JSON path.
Publisher and subscriber registration take the declared port name, type name, and
mandatory uint64 schema. Inputs use exact sources from the prepared contract.
Register all declared ports before `Session::ready()`. The smoke uses schema 42.
