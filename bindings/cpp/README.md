# C++17 bridge

`crates/reiny-ffi/include/reiny.hpp` wraps the additive C ABI with move-only
RAII handles, `std::optional` receive results and metadata, binary vectors, and
exceptions containing the facade's error text. Include it and link the same
shared library as C. `Session::open` supports the network backend and optional
Zenoh configuration; `LocalBus::connect` shares an in-process bus.

```sh
c++ -std=c++17 -Wall -Wextra -Werror -Icrates/reiny-ffi/include bindings/cpp/smoke.cpp \
  -Ltarget/debug -lreiny_ffi -o target/debug/cpp-smoke
LD_LIBRARY_PATH=target/debug target/debug/cpp-smoke
```

On macOS use `DYLD_LIBRARY_PATH=target/debug`. With an initialized Windows
MSVC developer prompt:

```bat
cl /nologo /EHsc /std:c++17 /W4 /WX /Icrates\reiny-ffi\include bindings\cpp\smoke.cpp /Fetarget\debug\cpp-smoke.exe /Fotarget\debug\cpp-smoke.obj /link target\debug\reiny_ffi.dll.lib
target\debug\cpp-smoke.exe
```

Each successful raw handle passed to a wrapper constructor transfers ownership
to that wrapper. Do not wrap a handle twice. `get()` only borrows a raw handle;
do not release it manually. Buffer copies and message getter results remain
valid independently of the message. Do not concurrently destroy a wrapper
while another thread uses it.
