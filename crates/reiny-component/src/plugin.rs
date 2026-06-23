//! プラグイン SDK の本体。任意の [`Component`](crate::Component) が、自分専用の名前空間
//! `hos/<id>/...` を持つ「プラグインインスタンス」として hs-gui / 他コンポーネントへ顔を出す。
//! `gui` フィーチャでのみ有効。
//!
//! インスタンスは最大 3 つの facet を持つ:
//! - **gui**     — [`GuiPanel`] で宣言した画面。layout/data/command を `hos/<id>/gui/*` で扱う。
//! - **topics**  — 汎用 named pub/sub。[`publish_topic`](PluginHandle::publish_topic) /
//!   [`subscribe_topic`](PluginHandle::subscribe_topic) で `hos/<id>/topics/<name>` をやり取りする。
//! - **configs** — [`config_file`](Plugin::config_file) で読んだ TOML を `hos/<id>/configs` へ
//!   外向きに publish(`RobotConfigBundle` 同形・読取専用)。
//!
//! **採番**: [`Plugin::new`] には基底名(例 `system-monitor`)を渡す。[`serve`](Plugin::serve)
//! 時に既存インスタンスの liveliness token(`hos/*`)を走査し、`<基底名>-<N>` の空き最小 N
//! (1 から)を取って `id`(例 `system-monitor-1`)を確定する。生存は token `hos/<id>` で表し、
//! drop(プロセス終了)で占有が解け、hs-gui のタブも消える。
//!
//! ```no_run
//! use reiny_component::gui::GuiPanel;
//! use reiny_component::plugin::Plugin;
//! # use reiny_component::Shutdown;
//! # fn run(shutdown: Shutdown) -> anyhow::Result<()> {
//! let panel = GuiPanel::new("System Monitor").gauge("cpu", "CPU", "cpu", 0.0, 100.0, "%");
//! let plugin = Plugin::new("system-monitor")
//!     .gui(panel)
//!     .config_file("configs/system-monitor/default.toml")?
//!     .serve(shutdown.clone())?;
//! tracing::info!("resolved instance id = {}", plugin.id());
//! while !shutdown.is_triggered() {
//!     plugin.set_number("cpu", 42.0);
//!     plugin.publish_topic_number("heartbeat", 1.0);
//!     std::thread::sleep(std::time::Duration::from_millis(50));
//! }
//! # Ok(()) }
//! ```

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use tokio::sync::mpsc as tmpsc;

use reiny_proto::{
    PluginCommand, PluginData, PluginLayout, PluginValue, RobotConfigBundle,
};
use reiny_transport::{
    HosSession, PresenceToken, ZenohPublisher, ZenohSubscriber, scan_alive, topics,
};

use crate::Shutdown;
use crate::gui::{GuiPanel, value_array, value_flag, value_number, value_text};

/// layout / config の再 announce 間隔(後から起動した購読者が拾えるように低レート)。
const ANNOUNCE_PERIOD: Duration = Duration::from_secs(1);
/// GUI データ publish 間隔(表示に十分な 20Hz)。
const DATA_PERIOD: Duration = Duration::from_millis(50);

/// プラグインインスタンスのビルダ。基底名で開始し、facet を足して [`serve`](Self::serve) する。
pub struct Plugin {
    base_name: String,
    layout: Option<PluginLayout>,
    config: Option<RobotConfigBundle>,
}

impl Plugin {
    /// 基底名(連番が付く前の名前。例 `system-monitor`)でインスタンスを開始する。
    pub fn new(base_name: impl Into<String>) -> Self {
        Self {
            base_name: base_name.into(),
            layout: None,
            config: None,
        }
    }

    /// GUI facet を付ける([`GuiPanel`] で構成を宣言)。付けない場合 hs-gui にタブは出ない。
    pub fn gui(mut self, panel: GuiPanel) -> Self {
        self.layout = Some(panel.into_layout());
        self
    }

    /// configs facet を付ける。`path` の TOML を読み、`hos/<id>/configs` へ生テキストのまま
    /// (`RobotConfigBundle` 同形で)外向きに publish する。robot config と同じく TOML が真実の源。
    pub fn config_file(mut self, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let toml = std::fs::read_to_string(path)
            .with_context(|| format!("read plugin config file {}", path.display()))?;
        let abs = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let config_dir = abs
            .parent()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        self.config = Some(RobotConfigBundle {
            toml,
            config_dir,
            config_path: abs.display().to_string(),
        });
        Ok(self)
    }

    /// configs facet を付ける(インメモリの TOML テキストから)。
    pub fn config_toml(mut self, toml: impl Into<String>) -> Self {
        self.config = Some(RobotConfigBundle {
            toml: toml.into(),
            config_dir: String::new(),
            config_path: String::new(),
        });
        self
    }

    /// インスタンスを起動する。専用スレッドで採番 → token 宣言 → 各 facet の駆動を行い、
    /// 採番済み id が確定してから [`PluginHandle`] を返す(id 確定までブロックする)。
    pub fn serve(self, shutdown: Shutdown) -> Result<PluginHandle> {
        let base = self.base_name;
        let layout = self.layout;
        let config = self.config;

        let gui_values: Option<Arc<Mutex<HashMap<String, PluginValue>>>> = layout
            .as_ref()
            .map(|_| Arc::new(Mutex::new(HashMap::new())));
        let (cmd_tx, cmd_rx) = mpsc::channel::<PluginCommand>();
        let (topic_tx, topic_rx) = tmpsc::unbounded_channel::<TopicOp>();
        let (init_tx, init_rx) = mpsc::channel::<Result<String>>();

        let gui_values_bg = gui_values.clone();
        let join = std::thread::Builder::new()
            .name(format!("plugin-{base}"))
            .spawn(move || {
                let rt = match tokio::runtime::Runtime::new() {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = init_tx.send(Err(anyhow!("build tokio runtime: {e}")));
                        return;
                    }
                };
                rt.block_on(async move {
                    if let Err(e) = serve_async(
                        base,
                        layout,
                        config,
                        gui_values_bg,
                        cmd_tx,
                        topic_rx,
                        init_tx,
                        shutdown,
                    )
                    .await
                    {
                        tracing::error!("plugin serve loop ended with error: {e:#}");
                    }
                });
            })
            .context("spawn plugin thread")?;

        // 採番(id 確定)と token 宣言が済むまで待つ。失敗は背景タスクから伝播する。
        let id = init_rx
            .recv()
            .context("plugin background thread died during init")??;

        Ok(PluginHandle {
            id,
            gui: gui_values.map(|values| GuiFacet {
                values,
                commands: cmd_rx,
            }),
            topic_ops: topic_tx,
            _join: join,
        })
    }
}

/// GUI facet 用の共有状態(値ストアと操作受信)。
struct GuiFacet {
    values: Arc<Mutex<HashMap<String, PluginValue>>>,
    commands: Receiver<PluginCommand>,
}

/// 稼働中プラグインインスタンスへのハンドル。値更新・操作取得・topics 入出力を行う。
/// drop すると背景タスクの token も落ち、占有とタブが解放される。
pub struct PluginHandle {
    id: String,
    gui: Option<GuiFacet>,
    topic_ops: tmpsc::UnboundedSender<TopicOp>,
    // 背景スレッド(join せず shutdown で自然終了)。
    _join: std::thread::JoinHandle<()>,
}

impl PluginHandle {
    /// 採番済みのインスタンス id(例 `system-monitor-1`)。topics/configs のキーにも使われる。
    pub fn id(&self) -> &str {
        &self.id
    }

    // ---- GUI facet: 値バインド / 操作取得(GUI facet が無いときは no-op / None)----

    /// `key` に [`PluginValue`] をバインドする(次の publish で hs-gui に反映)。
    pub fn set_value(&self, key: &str, value: PluginValue) {
        if let Some(g) = &self.gui {
            g.values
                .lock()
                .expect("gui plugin values mutex poisoned")
                .insert(key.to_string(), value);
        }
    }
    /// 数値をバインドする。
    pub fn set_number(&self, key: &str, n: f64) {
        self.set_value(key, value_number(n));
    }
    /// 真偽をバインドする。
    pub fn set_flag(&self, key: &str, b: bool) {
        self.set_value(key, value_flag(b));
    }
    /// 文字列をバインドする。
    pub fn set_text(&self, key: &str, s: impl Into<String>) {
        self.set_value(key, value_text(s.into()));
    }

    /// 受信済みの操作コマンドを 1 つ取り出す(無ければ `None`)。同期ループから呼ぶ。
    pub fn try_command(&self) -> Option<PluginCommand> {
        self.gui.as_ref().and_then(|g| g.commands.try_recv().ok())
    }

    // ---- topics facet: 汎用 named pub/sub ----

    /// 自分の名前空間 `hos/<id>/topics/<name>` へ値を publish する(publisher は遅延生成)。
    pub fn publish_topic(&self, name: &str, value: PluginValue) {
        let _ = self.topic_ops.send(TopicOp::Publish {
            name: name.to_string(),
            value,
        });
    }
    /// 数値トピックを publish する。
    pub fn publish_topic_number(&self, name: &str, n: f64) {
        self.publish_topic(name, value_number(n));
    }
    /// 真偽トピックを publish する。
    pub fn publish_topic_flag(&self, name: &str, b: bool) {
        self.publish_topic(name, value_flag(b));
    }
    /// 文字列トピックを publish する。
    pub fn publish_topic_text(&self, name: &str, s: impl Into<String>) {
        self.publish_topic(name, value_text(s.into()));
    }
    /// float 配列トピックを publish する(policy の obs/action/joint_target など、スカラに
    /// 収まらない構造化データを topics facet で運ぶ)。
    pub fn publish_topic_array(&self, name: &str, values: Vec<f64>) {
        self.publish_topic(name, value_array(values));
    }

    /// 同名トピックを **全インスタンス横断**(`hos/*/topics/<name>`)で購読する。連番 (-N) に
    /// 縛られず name で受けられるのでプラグイン間連携に向く。返り値の同期 [`Receiver`] を
    /// `try_recv` / `recv` で読む。背景タスクが subscriber を立て終えるまでブロックする。
    pub fn subscribe_topic(&self, name: &str) -> Receiver<PluginValue> {
        let (reply_tx, reply_rx) = mpsc::channel();
        let _ = self.topic_ops.send(TopicOp::Subscribe {
            name: name.to_string(),
            reply: reply_tx,
        });
        reply_rx
            .recv()
            .expect("plugin background task dropped before topic subscription was set up")
    }
}

/// ハンドル → 背景タスクへの topics 操作要求。
enum TopicOp {
    Publish {
        name: String,
        value: PluginValue,
    },
    Subscribe {
        name: String,
        reply: mpsc::Sender<Receiver<PluginValue>>,
    },
}

/// 既存インスタンスの liveliness を走査し、`<base>-<N>` の空き最小 N(1 から)で id を確定する。
async fn resolve_instance_id(session: &HosSession, base: &str) -> String {
    let alive = scan_alive(session, topics::PLUGIN_INSTANCE_ALL)
        .await
        .unwrap_or_default();
    let prefix = format!("{base}-");
    let taken: HashSet<u32> = alive
        .iter()
        .filter_map(|key| key.strip_prefix("hos/")) // "hos/<id>" -> "<id>"
        .filter_map(|id| id.strip_prefix(&prefix)) // "<base>-<N>" -> "<N>"
        .filter_map(|n| n.parse::<u32>().ok())
        .collect();
    let mut n = 1u32;
    while taken.contains(&n) {
        n += 1;
    }
    format!("{base}-{n}")
}

/// 確定済み facet 群とトークンを保持して背景ループを回すための一式。
struct Running {
    // tokens / session は drop されると占有が解けるので、ループ中ずっと保持する。
    _session: HosSession,
    _instance_token: PresenceToken,
    _gui_token: Option<PresenceToken>,
    id: String,
    layout: Option<PluginLayout>,
    layout_pub: Option<ZenohPublisher<PluginLayout>>,
    data_pub: Option<ZenohPublisher<PluginData>>,
    cmd_sub: Option<ZenohSubscriber<PluginCommand>>,
    config: Option<RobotConfigBundle>,
    config_pub: Option<ZenohPublisher<RobotConfigBundle>>,
}

/// 背景タスク本体: 採番 → token 宣言 → 各 facet 初期 announce →(id を報告して)再 announce /
/// data publish / command 受信 / topics 操作を 1 つの select ループで駆動する。
#[allow(clippy::too_many_arguments)]
async fn serve_async(
    base: String,
    layout: Option<PluginLayout>,
    config: Option<RobotConfigBundle>,
    gui_values: Option<Arc<Mutex<HashMap<String, PluginValue>>>>,
    cmd_tx: mpsc::Sender<PluginCommand>,
    mut topic_rx: tmpsc::UnboundedReceiver<TopicOp>,
    init_tx: mpsc::Sender<Result<String>>,
    shutdown: Shutdown,
) -> Result<()> {
    // ---- 初期化(採番 + token + 各 facet の宣言)。失敗は init_tx 経由で serve() へ返す ----
    let init: Result<Running> = async {
        let session = HosSession::open().await.context("open zenoh session")?;
        let id = resolve_instance_id(&session, &base).await;

        // インスタンス生存 token(id 占有マーカ)。
        let instance_token =
            PresenceToken::declare(&session, topics::plugin_instance_liveliness(&id))
                .await
                .context("declare instance liveliness token")?;

        // gui facet
        let mut gui_token = None;
        let mut layout_pub = None;
        let mut data_pub = None;
        let mut cmd_sub = None;
        let mut layout_msg = None;
        if let Some(mut l) = layout {
            l.plugin_id = id.clone(); // 採番済み id を注入
            gui_token = Some(
                PresenceToken::declare(&session, topics::plugin_gui_liveliness(&id))
                    .await
                    .context("declare gui liveliness token")?,
            );
            let lp = ZenohPublisher::<PluginLayout>::new(&session, topics::plugin_gui_layout(&id))
                .await?;
            lp.put(&l).await.context("initial layout announce")?;
            data_pub = Some(
                ZenohPublisher::<PluginData>::new(&session, topics::plugin_gui_data(&id)).await?,
            );
            cmd_sub = Some(
                ZenohSubscriber::<PluginCommand>::new(&session, topics::plugin_gui_command(&id))
                    .await?,
            );
            tracing::info!("plugin '{id}' gui announced ({} widgets)", l.widgets.len());
            layout_msg = Some(l);
            layout_pub = Some(lp);
        }

        // configs facet
        let mut config_pub = None;
        if let Some(bundle) = &config {
            let cp = ZenohPublisher::<RobotConfigBundle>::new(&session, topics::plugin_config(&id))
                .await?;
            cp.put(bundle).await.context("initial config announce")?;
            tracing::info!(
                "plugin '{id}' config announced ({} bytes toml)",
                bundle.toml.len()
            );
            config_pub = Some(cp);
        }

        Ok(Running {
            _session: session,
            _instance_token: instance_token,
            _gui_token: gui_token,
            id,
            layout: layout_msg,
            layout_pub,
            data_pub,
            cmd_sub,
            config,
            config_pub,
        })
    }
    .await;

    let st = match init {
        Ok(st) => {
            let _ = init_tx.send(Ok(st.id.clone()));
            st
        }
        Err(e) => {
            let _ = init_tx.send(Err(anyhow!("{e:#}")));
            return Err(e);
        }
    };

    // ---- 駆動ループ ----
    let mut announce = tokio::time::interval(ANNOUNCE_PERIOD);
    let mut data_tick = tokio::time::interval(DATA_PERIOD);
    let mut topic_pubs: HashMap<String, ZenohPublisher<PluginValue>> = HashMap::new();
    let mut topic_closed = false;

    loop {
        tokio::select! {
            _ = shutdown.wait() => break,
            // layout / config の低レート再 announce(後から起動した購読者向け)。
            _ = announce.tick() => {
                if let (Some(lp), Some(l)) = (&st.layout_pub, &st.layout) {
                    let _ = lp.put(l).await;
                }
                if let (Some(cp), Some(b)) = (&st.config_pub, &st.config) {
                    let _ = cp.put(b).await;
                }
            }
            // GUI データの定期 publish。
            _ = data_tick.tick() => {
                if let (Some(dp), Some(values)) = (&st.data_pub, &gui_values) {
                    let snapshot = values.lock().expect("values mutex poisoned").clone();
                    let msg = PluginData { plugin_id: st.id.clone(), timestamp: None, values: snapshot };
                    let _ = dp.put(&msg).await;
                }
            }
            // 操作コマンドの受信(GUI facet が無いと cmd_sub は None → pending で発火しない)。
            cmd = async {
                match &st.cmd_sub {
                    Some(s) => s.recv_async().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(c) = cmd { let _ = cmd_tx.send(c); }
            }
            // topics 操作要求(全ハンドルが drop されたら閉じ、以後は発火させない)。
            op = topic_rx.recv(), if !topic_closed => {
                match op {
                    Some(op) => handle_topic_op(op, &st._session, &st.id, &mut topic_pubs).await,
                    None => topic_closed = true,
                }
            }
        }
    }
    tracing::info!("plugin '{}' stopped", st.id);
    Ok(())
}

/// topics 操作を 1 件処理する(publisher は遅延生成・使い回し、subscriber は wildcard で立てる)。
async fn handle_topic_op(
    op: TopicOp,
    session: &HosSession,
    id: &str,
    topic_pubs: &mut HashMap<String, ZenohPublisher<PluginValue>>,
) {
    match op {
        TopicOp::Publish { name, value } => {
            if !topic_pubs.contains_key(&name) {
                match ZenohPublisher::<PluginValue>::new(session, topics::plugin_topic(id, &name))
                    .await
                {
                    Ok(p) => {
                        topic_pubs.insert(name.clone(), p);
                    }
                    Err(e) => {
                        tracing::error!(
                            "plugin '{id}' failed to create topic publisher '{name}': {e}"
                        );
                        return;
                    }
                }
            }
            if let Some(p) = topic_pubs.get(&name) {
                let _ = p.put(&value).await;
            }
        }
        TopicOp::Subscribe { name, reply } => {
            match ZenohSubscriber::<PluginValue>::new(session, topics::plugin_topic_any(&name))
                .await
            {
                Ok(sub) => {
                    let (tx, rx) = mpsc::channel();
                    tokio::spawn(async move {
                        while let Some(v) = sub.recv_async().await {
                            if tx.send(v).is_err() {
                                break; // 購読者(プラグイン)が Receiver を drop
                            }
                        }
                    });
                    let _ = reply.send(rx);
                }
                Err(e) => {
                    tracing::error!("plugin '{id}' failed to subscribe topic '{name}': {e}");
                    // reply を drop すると subscribe_topic 側の recv が panic するので、
                    // 失敗時は空のチャネル(送信端を即 drop)を返して握り潰す。
                    let (_dead_tx, dead_rx) = mpsc::channel();
                    let _ = reply.send(dead_rx);
                }
            }
        }
    }
}
