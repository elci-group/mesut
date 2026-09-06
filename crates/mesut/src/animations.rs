//! Terminal animations for Mesut output.
//!
//! The visual language is inspired by Mesut Özil's calm, precise playmaking:
//! a scanning eye, a clean passing lane, and a small burst when the result
//! arrives. Runtime lifecycle animations are enabled by default through
//! `AssistObserver`; this module provides a richer completion flourish for
//! command-line applications.

use std::io::{self, Write};
use std::thread;
use std::time::Duration;

const RESET: &str = "\x1b[0m";
const GOLD: &str = "\x1b[38;5;220m";
const TEAL: &str = "\x1b[38;5;45m";
const DIM: &str = "\x1b[2m";

/// A compact, Özil-inspired terminal output animation.
#[derive(Debug, Clone)]
pub struct AssistAnimation {
    label: String,
    frame_delay: Duration,
    color: bool,
    motion: bool,
}

impl AssistAnimation {
    /// Creates an animation labelled with the result being delivered.
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            frame_delay: Duration::from_millis(55),
            color: std::env::var_os("NO_COLOR").is_none(),
            motion: true,
        }
    }

    /// Sets the delay between frames. Useful for tests and slower terminals.
    pub fn frame_delay(mut self, delay: Duration) -> Self {
        self.frame_delay = delay;
        self
    }

    /// Enables or disables ANSI color while retaining the same visual states.
    pub fn color(mut self, enabled: bool) -> Self {
        self.color = enabled;
        self
    }

    /// Disables intermediate motion frames for reduced-motion environments.
    pub fn motion(mut self, enabled: bool) -> Self {
        self.motion = enabled;
        self
    }

    /// Plays the animation into any writer, making it easy to use with stdout
    /// or a captured buffer in tests. Returns the writer's first I/O error.
    pub fn play<W: Write>(&self, out: &mut W) -> io::Result<()> {
        let paint = |color: &str, value: &str| {
            if self.color {
                format!("{color}{value}{RESET}")
            } else {
                value.to_owned()
            }
        };
        let frames = [
            format!("  {}  scanning the pitch  ·", paint(DIM, "◌")),
            format!("  {}  scanning the pitch  ·  ·", paint(TEAL, "◔")),
            format!("  {}  scanning the pitch  ·  ·  ·", paint(TEAL, "◑")),
            format!("  {}  lane spotted       ╭", paint(TEAL, "◉")),
            format!("  {}  shaping the pass   ╰─", paint(GOLD, "◆")),
            format!("  {}  shaping the pass   ╰───", paint(GOLD, "◆")),
            format!("  {}  threading the assist   ━━━▶", paint(GOLD, "◆")),
            format!("  {}  threading the assist      ━━━▶", paint(GOLD, "◆")),
            format!("  {}  {}", paint(GOLD, "✦"), paint(GOLD, &self.label)),
        ];
        let visible_frames: &[String] = if self.motion {
            &frames
        } else {
            &frames[frames.len() - 1..]
        };
        for (index, frame) in visible_frames.iter().enumerate() {
            write!(out, "\r\x1b[2K{frame}")?;
            out.flush()?;
            if index + 1 < visible_frames.len() {
                thread::sleep(self.frame_delay);
            }
        }
        writeln!(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_the_assist_and_result() {
        let mut output = Vec::new();
        AssistAnimation::new("result ready")
            .frame_delay(Duration::ZERO)
            .color(false)
            .play(&mut output)
            .unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("threading the assist"));
        assert!(output.contains("result ready"));
        assert!(output.ends_with("\n"));
    }

    #[test]
    fn reduced_motion_renders_only_the_result() {
        let mut output = Vec::new();
        AssistAnimation::new("done")
            .motion(false)
            .color(false)
            .play(&mut output)
            .unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("done"));
        assert!(!output.contains("scanning"));
    }
}
