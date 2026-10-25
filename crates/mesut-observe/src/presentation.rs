use crate::{EventObserver, TaskEvent, TaskEventType};
use parking_lot::Mutex;
use std::env;
use std::io::{self, IsTerminal, Write};

const RESET: &str = "\x1b[0m";
const DIM: &str = "\x1b[2m";
const GOLD: &str = "\x1b[38;5;220m";
const TEAL: &str = "\x1b[38;5;45m";
const RED: &str = "\x1b[38;5;203m";
const CLEAR_LINE: &str = "\r\x1b[2K";

/// Default terminal presentation for Mesut task events.
///
/// It is lifecycle-driven rather than timer-driven, so animation never delays
/// task execution. In auto mode it renders only to an interactive terminal.
pub struct AssistObserver {
    enabled: bool,
    color: bool,
    render_lock: Mutex<()>,
}

impl AssistObserver {
    /// Creates the default adaptive observer.
    ///
    /// `MESUT_ANIMATIONS=always` forces rendering and
    /// `MESUT_ANIMATIONS=never` disables it.
    pub fn auto() -> Self {
        let preference = env::var("MESUT_ANIMATIONS")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let enabled = match preference.as_str() {
            "always" | "1" | "true" | "on" => true,
            "never" | "0" | "false" | "off" => false,
            _ => {
                io::stderr().is_terminal()
                    && env::var_os("CI").is_none()
                    && env::var("TERM").map_or(true, |term| term != "dumb")
            }
        };
        Self {
            enabled,
            color: env::var_os("NO_COLOR").is_none(),
            render_lock: Mutex::new(()),
        }
    }

    /// Creates an observer that always renders. Intended for demos and tools
    /// that own their terminal surface.
    pub fn always() -> Self {
        Self {
            enabled: true,
            color: env::var_os("NO_COLOR").is_none(),
            render_lock: Mutex::new(()),
        }
    }

    fn paint(&self, color: &'static str, text: &str) -> String {
        if self.color {
            format!("{color}{text}{RESET}")
        } else {
            text.to_owned()
        }
    }

    fn render(&self, event: &TaskEvent) -> (String, bool) {
        let id = event.task_id.to_string();
        let short_id = &id[..8.min(id.len())];
        let suffix = if self.color {
            format!(" {DIM}#{short_id}{RESET}")
        } else {
            format!(" #{short_id}")
        };

        match &event.event_type {
            TaskEventType::Submitted { work_kind } => (
                format!(
                    "{}  scanning the pitch · {work_kind:?}{suffix}",
                    self.paint(TEAL, "◌")
                ),
                false,
            ),
            TaskEventType::Routed { route } => (
                format!("{}  lane spotted · {route}{suffix}", self.paint(TEAL, "◉")),
                false,
            ),
            TaskEventType::Queued { queue_depth } => (
                format!(
                    "{}  shaping the pass {}▶ · queue {queue_depth}{suffix}",
                    self.paint(GOLD, "◆"),
                    self.paint(GOLD, "───")
                ),
                false,
            ),
            TaskEventType::Started { executor_id } => (
                format!(
                    "{}  assist delivered {}▶ {executor_id}{suffix}",
                    self.paint(GOLD, "✦"),
                    self.paint(GOLD, "━━━━")
                ),
                true,
            ),
            TaskEventType::Completed {
                duration_ms,
                phases,
            } => {
                let execution = phases
                    .map(|p| p.execution_us)
                    .map(|us| format!(" · exec {}ms", us / 1000))
                    .unwrap_or_default();
                (
                    format!(
                        "{}  finished in {duration_ms}ms{execution}{suffix}",
                        self.paint(GOLD, "✓")
                    ),
                    true,
                )
            }
            TaskEventType::Failed { error } => (
                format!(
                    "{}  move broke down · {error}{suffix}",
                    self.paint(RED, "×")
                ),
                true,
            ),
            TaskEventType::Cancelled { reason } => (
                format!("{}  run halted · {reason}{suffix}", self.paint(RED, "■")),
                true,
            ),
        }
    }
}

impl Default for AssistObserver {
    fn default() -> Self {
        Self::auto()
    }
}

impl EventObserver for AssistObserver {
    fn observe(&self, event: &TaskEvent) {
        if !self.enabled {
            return;
        }

        let _guard = self.render_lock.lock();
        let (line, terminal) = self.render(event);
        let mut stderr = io::stderr().lock();
        let _ = write!(stderr, "{CLEAR_LINE}{line}");
        if terminal {
            let _ = writeln!(stderr);
        }
        let _ = stderr.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mesut_core::{TaskId, WorkKind};

    #[test]
    fn event_copy_is_compact_and_lifecycle_specific() {
        let observer = AssistObserver::always();
        let task_id = TaskId::new();
        let (submitted, done) = observer.render(&TaskEvent::submitted(task_id, WorkKind::Io));
        assert!(submitted.contains("scanning the pitch"));
        assert!(!done);

        let (started, done) = observer.render(&TaskEvent::started(task_id, "tokio".into()));
        assert!(started.contains("assist delivered"));
        assert!(done);
    }
}
