// Included by the C++ step client: node callbacks and retained lane state.
enum class Mode {
    normal, blocked, standard_exception, unknown_exception,
    invalid_output, missing_output, prepare_exception, stop_exception
};
struct State {
    Mode mode = Mode::normal;
    bool first = false, block_destroy = false;
    std::shared_ptr<rc::Schema> schema, wrong;
    std::thread::id lane;
    Gate callback_gate, destroy_gate;
    std::atomic<unsigned> created{0}, prepared{0}, effects{0}, stopped{0}, destroyed{0};
    rc::Context context;
    rs::Inputs retained;
    std::optional<rs::OutputsView> expired_output;
};
static rc::Bytes number_bytes(unsigned number) {
    assert(number < 128);
    return {8, static_cast<uint8_t>(number)}; // Number { uint32 value = 1; }
}
static unsigned number(const rs::Value& value, const rc::Schema& schema) {
    auto data = value.bytes(schema);
    assert(data.size() == 2 && data[0] == 8 && data[1] < 128);
    return data[1];
}
class Node : public rs::Component {
    std::shared_ptr<State> state_;
    void lane() const { assert(state_->lane == std::this_thread::get_id()); }
public:
    Node(std::shared_ptr<State> state, rc::ContextView context) : state_(std::move(state)) {
        state_->lane = std::this_thread::get_id();
        assert(state_->created++ == 0);
        state_->context = context.retain();
    }
    ~Node() noexcept override {
        lane();
        assert(state_->stopped == 1);
        if (state_->block_destroy) state_->destroy_gate.block();
        assert(state_->destroyed++ == 0);
    }
    void prepare(rc::ContextView context) override {
        lane();
        assert(!context.info().text().empty());
        ++state_->prepared;
        if (state_->mode == Mode::prepare_exception)
            throw std::runtime_error("C++ step preparation exception");
    }
    void step(rc::ContextView context, const rs::Tick& tick,
              rs::InputsView inputs, rs::OutputsView outputs) override {
        lane();
        assert(!context.stopping());
        assert(tick.start.secs == origin && tick.dt.secs == 0 && tick.dt.nanos == 7);
        assert(tick.end.secs == origin && tick.end.nanos == tick.start.nanos + 7);
        ++state_->effects; // Cancellation must not pretend this effect rolls back.
        state_->retained = inputs.retain();
        state_->expired_output = outputs;
        if (state_->mode == Mode::blocked) state_->callback_gate.block();
        if (state_->mode == Mode::standard_exception)
            throw std::runtime_error("C++ step exception");
        if (state_->mode == Mode::unknown_exception) throw 17;
        if (state_->mode == Mode::missing_output) return;
        auto n = number(inputs.get("x"), *state_->schema);
        n += state_->first ? number(inputs.get("prev"), *state_->schema) : 1;
        auto value = rs::Value::create(
            state_->mode == Mode::invalid_output ? *state_->wrong : *state_->schema,
            number_bytes(n));
        // The native sink rejects writes from a different ordinary thread.
        auto off_lane = std::async(std::launch::async, [&] {
            expect(REINY_EXEC_INVALID_STATE, [&] { outputs.set("out", value); });
        });
        ready(off_lane);
        outputs.set("out", value);
        expect(REINY_EXEC_INVALID_STATE, [&] { outputs.set("out", value); });
    }
    void stop(rc::ContextView context, uint32_t) override {
        lane();
        assert(context.stopping());
        assert(state_->stopped++ == 0);
        if (state_->mode == Mode::stop_exception)
            throw std::runtime_error("C++ step stop exception");
    }
};
struct FactoryCapture {
    std::shared_ptr<std::atomic<unsigned>> released;
    ~FactoryCapture() noexcept { released->fetch_add(1); }
};
