//! Smoke test of the `ledgerlog` binary: serve, produce, consume with a group.

use std::io::Write;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_ledgerlog");

struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn run(args: &[&str], stdin: Option<&str>) -> (String, String) {
    let mut child = Command::new(BIN)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    if let Some(input) = stdin {
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
    } else {
        drop(child.stdin.take());
    }
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8(out.stdout).unwrap();
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(out.status.success(), "{args:?} failed: {stderr}");
    (stdout, stderr)
}

#[test]
fn serve_produce_consume_with_group() {
    let dir = tempfile::tempdir().unwrap();
    let addr = format!("127.0.0.1:{}", free_port());
    let _server = KillOnDrop(
        Command::new(BIN)
            .args(["serve", "--addr", &addr, "--fsync", "never", "--data-dir"])
            .arg(dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::net::TcpStream::connect(&addr).is_err() {
        assert!(Instant::now() < deadline, "server did not start");
        std::thread::sleep(Duration::from_millis(20));
    }

    run(
        &["create-topic", "--addr", &addr, "--topic", "greetings"],
        None,
    );
    run(
        &[
            "produce",
            "--addr",
            &addr,
            "--topic",
            "greetings",
            "--key",
            "k",
        ],
        Some("hello\nworld\nfrom\nledgerlog\n"),
    );

    let (out, _) = run(
        &[
            "consume",
            "--addr",
            &addr,
            "--topic",
            "greetings",
            "--group",
            "cli",
            "-n",
            "2",
        ],
        None,
    );
    assert_eq!(out, "0\tk\thello\n1\tk\tworld\n");

    // The group resumes where it stopped.
    let (out, _) = run(
        &[
            "consume",
            "--addr",
            &addr,
            "--topic",
            "greetings",
            "--group",
            "cli",
            "--values-only",
        ],
        None,
    );
    assert_eq!(out, "from\nledgerlog\n");

    let (out, _) = run(&["topics", "--addr", &addr], None);
    assert!(out
        .lines()
        .any(|l| l.starts_with("greetings") && l.contains(" 4 ")));
}
