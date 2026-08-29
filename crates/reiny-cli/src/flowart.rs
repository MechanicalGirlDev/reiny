//! The topic flow diagram — launches as vertical rails, types as horizontal arrows (sequence-diagram style).
//!
//! Drawn by hand rather than by adding a dependency. One launch = one column (a box plus a rail),
//! one type = one row of horizontal arrow. `●` = the sender, `▶ / ◀` = the arrowhead into a receiver,
//! `┼` = a crossing with an uninvolved rail. A `[services]` request type is drawn with a double line
//! `═` from caller ●══▶ server, with `Req => Reply` in the label. Colours cycle the basic ANSI colours
//! per type, switched on or off by whether the caller is a terminal (and by `NO_COLOR`).
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

/// One type's worth of edge. `pubs` / `subs` are column (launch) indices, and the arrow runs `pubs` → `subs`.
/// For a `[services]` request type the caller swaps the roles before handing it over
/// (`pubs` = the caller, `subs` = the server).
pub(crate) struct Edge {
    pub ty: String,
    pub pubs: Vec<usize>,
    pub subs: Vec<usize>,
    /// For a `[services]` request type, the reply type's name.
    pub reply: Option<String>,
}

impl Edge {
    /// The label drawn on the row. A service shows its reply too, so the row reads in one go.
    fn label(&self) -> String {
        self.reply
            .as_ref()
            .map_or_else(|| self.ty.clone(), |r| format!("{} => {}", self.ty, r))
    }
}

/// The ANSI foreground colours assigned to types (rows) in a cycle.
const PALETTE: [&str; 6] = ["36", "35", "33", "32", "34", "91"];
/// The style of the frame and the rails (dim).
const FRAME: u8 = 0;
/// The style of a launch's name (bold).
const NAME: u8 = 255;

/// One row's canvas: the characters, plus a style per cell (FRAME / NAME / a 1-based palette number).
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

    /// Trim the trailing blanks into one line of text. With `color`, wrap each run of one style in
    /// ANSI escapes (a blank cell does not force a style change).
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

/// Draw the flow diagram as a sequence of rows. `names` are the columns (launches), `edges` the rows (types).
/// With either empty, nothing is drawn.
pub(crate) fn render(names: &[String], edges: &[Edge], color: bool) -> Vec<String> {
    if names.is_empty() || edges.is_empty() {
        return Vec::new();
    }

    // A box is as wide as its name + 4, rounded up to odd so the rail runs down its middle.
    let boxw: Vec<usize> = names.iter().map(|n| (n.chars().count() + 4) | 1).collect();
    let maxw = boxw.iter().copied().max().unwrap_or(0);
    let maxlabel = edges
        .iter()
        .map(|e| e.label().chars().count())
        .max()
        .unwrap_or(0);
    // The column spacing: wide enough that boxes do not touch and most labels fit between adjacent rails (anything longer spills into the right margin).
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

    // The heading and the boxes.
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

    // One arrow row per type (with a rail row in between).
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

/// Draw one edge into `row`. Returns the label that could not be embedded in the line (or whose side
/// is missing), for the caller to place in the right margin.
fn draw_edge(row: &mut Line, e: &Edge, centers: &[usize], style: u8) -> Option<String> {
    let lc = if e.reply.is_some() { '═' } else { '─' };
    let text = e.label();

    if e.pubs.is_empty() {
        // No sender: draw only an arrowhead into the leftmost receiver.
        let c = centers[e.subs[0]];
        row.fill(c - 4, c - 2, lc, style);
        row.put(c - 1, '▶', style);
        return Some(format!("{text}  (no publisher)"));
    }
    if e.subs.is_empty() {
        // No receiver: make it an arrow running off to the right.
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
    // Rails not involved in this edge are crossed (┼).
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
    // The label: embedded in the line when it fits between the leftmost rail and the next one along.
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
#[allow(clippy::expect_used, clippy::unwrap_used)] // tests may fail by panicking
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
        // a → c crosses b's rail with ┼. Lost has no receiver and runs off to the right.
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
