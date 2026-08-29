//! トピックの流れ図 —— launch を縦レール、型を横矢印で描く(sequence-diagram 風)。
//!
//! 依存を増やさず自前で描く。1 launch = 1 列(箱 + 縦レール)、1 型 = 1 行の横矢印。
//! `●` = 送り手、`▶ / ◀` = 受け手へ向かう矢先、`┼` = 関係しないレールとの交差。
//! `[services]` の request 型は二重線 `═` で caller ●══▶ server の向き、ラベルに
//! `Req => Reply` を添える。色は型ごとに ANSI の基本色を巡回し、呼び出し側が
//! 端末かどうか(と `NO_COLOR`)で on/off する。
//!
//! ```text
//!  topic flow
//!  ╭───────╮    ╭───────╮
//!  │ ping  │    │ pong  │
//!  ╰───┬───╯    ╰───┬───╯
//!      │            │
//!      ●─── Ping ──▶│
//!      │            │
//!      │◀── Pong ───●
//!      │            │
//! ```

/// 1 型ぶんの辺。`pubs` / `subs` は列(launch)の添字で、矢印は `pubs` → `subs` に引く。
/// service の request 型は呼び出し側が caller / server を入れ替えてから渡す
/// (`pubs` = caller、`subs` = server)。
pub(crate) struct Edge {
    pub ty: String,
    pub pubs: Vec<usize>,
    pub subs: Vec<usize>,
    /// `[services]` の request 型なら reply の型名。
    pub reply: Option<String>,
}

impl Edge {
    /// 行に描くラベル。service は reply まで一息で読めるようにする。
    fn label(&self) -> String {
        self.reply
            .as_ref()
            .map_or_else(|| self.ty.clone(), |r| format!("{} => {}", self.ty, r))
    }
}

/// 型(行)に巡回で割り当てる ANSI 前景色。
const PALETTE: [&str; 6] = ["36", "35", "33", "32", "34", "91"];
/// 枠とレールのスタイル(dim)。
const FRAME: u8 = 0;
/// launch 名のスタイル(bold)。
const NAME: u8 = 255;

/// 1 行ぶんのキャンバス。文字と、セルごとのスタイル(FRAME / NAME / 1 始まりのパレット番号)。
struct Line {
    ch: Vec<char>,
    st: Vec<u8>,
}

impl Line {
    fn new(width: usize) -> Self {
        Line {
            ch: vec![' '; width],
            st: vec![FRAME; width],
        }
    }

    fn put(&mut self, x: usize, c: char, s: u8) {
        if x < self.ch.len() {
            self.ch[x] = c;
            self.st[x] = s;
        }
    }

    fn fill(&mut self, a: usize, b: usize, c: char, s: u8) {
        for x in a..=b {
            self.put(x, c, s);
        }
    }

    fn text(&mut self, x: usize, t: &str, s: u8) {
        for (i, c) in t.chars().enumerate() {
            self.put(x + i, c, s);
        }
    }

    /// 末尾の空白を落として 1 行の文字列にする。`color` なら同スタイルの連なりごとに
    /// ANSI エスケープを挟む(空白セルはスタイル切り替えを起こさない)。
    fn render(&self, color: bool) -> String {
        let end = self.ch.iter().rposition(|&c| c != ' ').map_or(0, |i| i + 1);
        if !color {
            return self.ch[..end].iter().collect();
        }
        let mut out = String::new();
        let mut cur: Option<u8> = None;
        for (&c, &s) in self.ch[..end].iter().zip(&self.st) {
            if c != ' ' && cur != Some(s) {
                out.push_str("\x1b[0m");
                match s {
                    FRAME => out.push_str("\x1b[2m"),
                    NAME => out.push_str("\x1b[1m"),
                    k => {
                        out.push_str("\x1b[");
                        out.push_str(PALETTE[(k as usize - 1) % PALETTE.len()]);
                        out.push('m');
                    }
                }
                cur = Some(s);
            }
            out.push(c);
        }
        out.push_str("\x1b[0m");
        out
    }
}

/// 流れ図を行の列として描く。`names` が列(launch)、`edges` が行(型)。
/// どちらかが空なら何も描かない。
pub(crate) fn render(names: &[String], edges: &[Edge], color: bool) -> Vec<String> {
    if names.is_empty() || edges.is_empty() {
        return Vec::new();
    }

    // 箱の幅は名前 + 4。レールを中央に通すため奇数へ切り上げる。
    let boxw: Vec<usize> = names.iter().map(|n| (n.chars().count() + 4) | 1).collect();
    let maxw = boxw.iter().copied().max().unwrap_or(0);
    let maxlabel = edges
        .iter()
        .map(|e| e.label().chars().count())
        .max()
        .unwrap_or(0);
    // 列間隔: 箱が重ならず、たいていのラベルが隣接レール間に収まる幅(長すぎる分は右余白へ)。
    let pitch = (maxw + 4).max((maxlabel + 6).min(30)).max(12);
    let centers: Vec<usize> = (0..names.len()).map(|i| 1 + maxw / 2 + i * pitch).collect();
    let last = centers[centers.len() - 1];
    let width = last + pitch / 2 + maxlabel + 24;

    let rails = || {
        let mut l = Line::new(width);
        for &c in &centers {
            l.put(c, '│', FRAME);
        }
        l
    };

    // 見出しと箱。
    let mut title = Line::new(width);
    title.text(1, "topic flow", NAME);
    let mut top = Line::new(width);
    let mut mid = Line::new(width);
    let mut bot = Line::new(width);
    for (i, &c) in centers.iter().enumerate() {
        let h = boxw[i] / 2;
        top.put(c - h, '╭', FRAME);
        top.fill(c - h + 1, c + h - 1, '─', FRAME);
        top.put(c + h, '╮', FRAME);
        mid.put(c - h, '│', FRAME);
        mid.put(c + h, '│', FRAME);
        let len = names[i].chars().count();
        mid.text(c - h + 1 + (2 * h - 1 - len) / 2, &names[i], NAME);
        bot.put(c - h, '╰', FRAME);
        bot.fill(c - h + 1, c + h - 1, '─', FRAME);
        bot.put(c + h, '╯', FRAME);
        bot.put(c, '┬', FRAME);
    }
    let mut lines = vec![title, top, mid, bot, rails()];

    // 型ごとの矢印行(行間にレールを 1 行挟む)。
    for (k, e) in edges.iter().enumerate() {
        let style = u8::try_from(k % PALETTE.len() + 1).unwrap_or(1);
        let mut row = rails();
        if let Some(note) = draw_edge(&mut row, e, &centers, style) {
            row.text(last + 3, &note, style);
        }
        lines.push(row);
        lines.push(rails());
    }

    lines.iter().map(|l| l.render(color)).collect()
}

/// 1 本の辺を `row` に描く。線へ埋め込めなかった(または片側が居ない)ラベルを返す
/// (呼び出し側が右余白に置く)。
fn draw_edge(row: &mut Line, e: &Edge, centers: &[usize], style: u8) -> Option<String> {
    let lc = if e.reply.is_some() { '═' } else { '─' };
    let text = e.label();

    if e.pubs.is_empty() {
        // 送り手不在: 最左の受け手へ矢先だけ描く。
        let c = centers[e.subs[0]];
        row.fill(c - 4, c - 2, lc, style);
        row.put(c - 1, '▶', style);
        return Some(format!("{text}  (no publisher)"));
    }
    if e.subs.is_empty() {
        // 受け手不在: 右へ抜ける矢印にする。
        for &p in &e.pubs {
            row.put(centers[p], '●', style);
        }
        let c = centers[e.pubs[e.pubs.len() - 1]];
        row.fill(c + 1, c + 3, lc, style);
        row.put(c + 4, '▶', style);
        return Some(format!("{text}  (no subscriber)"));
    }

    let involved: Vec<usize> = e.pubs.iter().chain(&e.subs).copied().collect();
    let lo = involved.iter().map(|&i| centers[i]).min().unwrap_or(0);
    let hi = involved.iter().map(|&i| centers[i]).max().unwrap_or(0);
    row.fill(lo + 1, hi - 1, lc, style);
    // 辺に関与しないレールとは交差(┼)。
    for (i, &c) in centers.iter().enumerate() {
        if c > lo && c < hi && !involved.contains(&i) {
            row.put(c, '┼', style);
        }
    }
    for &s in &e.subs {
        let c = centers[s];
        if e.pubs.iter().any(|&p| centers[p] < c) {
            row.put(c - 1, '▶', style);
            if c < hi {
                row.put(c, '┼', style);
            }
        } else {
            row.put(c + 1, '◀', style);
            if c > lo {
                row.put(c, '┼', style);
            }
        }
    }
    for &p in &e.pubs {
        row.put(centers[p], '●', style);
    }
    // ラベル: 左端レールとその右隣のレールの間に収まれば線へ埋め込む。
    let next = centers
        .iter()
        .copied()
        .filter(|&c| c > lo)
        .min()
        .unwrap_or(hi);
    let pad = format!(" {text} ");
    let cap = next.saturating_sub(lo + 3);
    let len = pad.chars().count();
    if len <= cap {
        row.text(lo + 2 + (cap - len) / 2, &pad, style);
        None
    } else {
        Some(text)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)] // テストは panic で失敗を表現してよい
mod tests {
    use super::*;

    fn edge(ty: &str, pubs: &[usize], subs: &[usize], reply: Option<&str>) -> Edge {
        Edge {
            ty: ty.to_string(),
            pubs: pubs.to_vec(),
            subs: subs.to_vec(),
            reply: reply.map(ToString::to_string),
        }
    }

    fn names(items: &[&str]) -> Vec<String> {
        items.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn ping_pong_snapshot() {
        let lines = render(
            &names(&["ping", "pong"]),
            &[
                edge("Ping", &[0], &[1], None),
                edge("Pong", &[1], &[0], None),
            ],
            false,
        );
        assert_eq!(
            lines,
            vec![
                " topic flow",
                " ╭───────╮    ╭───────╮",
                " │ ping  │    │ pong  │",
                " ╰───┬───╯    ╰───┬───╯",
                "     │            │",
                "     ●─── Ping ──▶│",
                "     │            │",
                "     │◀── Pong ───●",
                "     │            │",
            ]
        );
    }

    #[test]
    fn service_uses_double_line_and_reply_label() {
        let lines = render(
            &names(&["asker", "calc"]),
            &[edge("Add", &[0], &[1], Some("Sum"))],
            false,
        );
        let row = lines.iter().find(|l| l.contains("Add")).expect("Add row");
        assert!(
            row.contains('═') && row.contains("Add => Sum") && row.contains('●'),
            "{row}"
        );
    }

    #[test]
    fn crossing_rails_and_missing_sides() {
        // a → c は b のレールを ┼ で跨ぐ。Lost は受け手なしで右へ抜ける。
        let lines = render(
            &names(&["a", "b", "c"]),
            &[
                edge("Far", &[0], &[2], None),
                edge("Lost", &[0], &[], None),
                edge("Ghost", &[], &[1], None),
            ],
            false,
        );
        let far = lines.iter().find(|l| l.contains("Far")).expect("Far row");
        assert!(far.contains('┼') && far.contains('▶'), "{far}");
        assert!(lines.iter().any(|l| l.contains("Lost  (no subscriber)")));
        assert!(lines.iter().any(|l| l.contains("Ghost  (no publisher)")));
    }

    #[test]
    fn color_wraps_and_resets() {
        let lines = render(&names(&["a", "b"]), &[edge("T", &[0], &[1], None)], true);
        let row = lines.iter().find(|l| l.contains('●')).expect("arrow row");
        assert!(
            row.contains("\x1b[36m") && row.ends_with("\x1b[0m"),
            "{row:?}"
        );
    }

    #[test]
    fn empty_input_renders_nothing() {
        assert!(render(&[], &[], false).is_empty());
        assert!(render(&names(&["a"]), &[], false).is_empty());
    }
}
