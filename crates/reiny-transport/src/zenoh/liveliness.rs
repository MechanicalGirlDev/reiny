//! Zenoh liveliness ヘルパ。プラグインの「生存」を token で表し、hs-gui がその出現/退場を
//! 観測してタブを動的に追加/削除するために使う。
//!
//! - [`PresenceToken`]: プラグイン側が宣言して保持する。保持中は当該キーが alive、drop すると
//!   (プロセス終了でも)購読側に退場として観測される。
//! - [`PresenceSubscriber`]: hs-gui 側がワイルドカードキーで購読し、各プラグインの出現/退場を
//!   [`PresenceEvent`] として受ける。`history(true)` で既に alive な token も再生されるため、
//!   後から起動した hs-gui も既存プラグインを把握できる。

use std::sync::Arc;

use zenoh::Session;
use zenoh::handlers::FifoChannelHandler;
use zenoh::liveliness::LivelinessToken;
use zenoh::pubsub::Subscriber;
use zenoh::sample::{Sample, SampleKind};

use super::HosSession;
use crate::CommError;

/// 宣言済みの liveliness token。保持している間だけキーが alive。
pub struct PresenceToken {
    _token: LivelinessToken,
    // セッションを生かしておく。
    _session: Arc<Session>,
}

impl PresenceToken {
    /// `key`(例: `hos/gui/<id>`)の liveliness token を宣言する。
    pub async fn declare(session: &HosSession, key: impl Into<String>) -> Result<Self, CommError> {
        let token = session
            .inner()
            .liveliness()
            .declare_token(key.into())
            .await?;
        Ok(Self {
            _token: token,
            _session: session.inner().clone(),
        })
    }
}

/// liveliness 購読が観測した出現/退場イベント。
#[derive(Debug, Clone)]
pub struct PresenceEvent {
    /// 観測したフルキー(例: `hos/gui/system-monitor`)。
    pub key: String,
    /// `true`=出現(Put) / `false`=退場(Delete)。
    pub alive: bool,
}

/// liveliness キー式(ワイルドカード可)の購読。
pub struct PresenceSubscriber {
    subscriber: Subscriber<FifoChannelHandler<Sample>>,
    // セッションを生かしておく。
    _session: Arc<Session>,
}

impl PresenceSubscriber {
    /// `key_expr`(例: `hos/gui/*`)で liveliness を購読する。`history(true)` により、購読開始
    /// 時点で既に alive な token も Put として再生される。
    pub async fn declare(
        session: &HosSession,
        key_expr: impl Into<String>,
    ) -> Result<Self, CommError> {
        let subscriber = session
            .inner()
            .liveliness()
            .declare_subscriber(key_expr.into())
            .history(true)
            .await?;
        Ok(Self {
            subscriber,
            _session: session.inner().clone(),
        })
    }

    /// 次の出現/退場イベントを受け取る。チャネルが閉じたら `None`。
    pub async fn recv_async(&self) -> Option<PresenceEvent> {
        match self.subscriber.recv_async().await {
            Ok(sample) => Some(PresenceEvent {
                key: sample.key_expr().as_str().to_string(),
                alive: matches!(sample.kind(), SampleKind::Put),
            }),
            Err(_) => None, // channel closed
        }
    }
}

/// 現在 alive な liveliness token のキー一覧を **一度だけ** 問い合わせる。
///
/// [`PresenceSubscriber`] が継続的に出現/退場を流すのに対し、これはその時点のスナップショットを
/// 返して即完了する。プラグイン起動時の連番採番(占有中の id 集合を集めて空き番号を選ぶ)に使う。
pub async fn scan_alive(session: &HosSession, key_expr: &str) -> Result<Vec<String>, CommError> {
    let replies = session.inner().liveliness().get(key_expr).await?;
    let mut keys = Vec::new();
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            keys.push(sample.key_expr().as_str().to_string());
        }
    }
    Ok(keys)
}
