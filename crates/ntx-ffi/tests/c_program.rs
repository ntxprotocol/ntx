//! Surface test: compile `examples/hello.c` against `include/ntx.h` and the built C library,
//! then run it. This checks the real C interface, not Rust calling Rust.
#![cfg(unix)] // Windows is covered by the Rust tests of the same functions

use std::path::{Path, PathBuf};
use std::process::Command;

/// The folder with `libntx_ffi`, next to this test executable.
fn library_dir() -> PathBuf {
    std::env::current_exe().unwrap().parent().unwrap().to_path_buf()
}

#[test]
fn hello_c_compiles_links_and_runs() {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let out = std::env::temp_dir().join(format!("ntx-hello-{}", std::process::id()));
    let libs = library_dir();
    let compiled = Command::new(std::env::var("CC").unwrap_or_else(|_| "cc".into()))
        .arg(crate_dir.join("examples/hello.c"))
        .arg("-std=c99")
        .args(["-Wall", "-Wextra", "-Werror"])
        .arg("-I")
        .arg(crate_dir.join("include"))
        .arg("-L")
        .arg(&libs)
        .arg(format!("-Wl,-rpath,{}", libs.display()))
        .arg("-lntx_ffi")
        .arg("-o")
        .arg(&out)
        .status()
        .expect("a C compiler (cc) is needed for this test");
    assert!(compiled.success(), "hello.c did not compile");

    let run = Command::new(&out).output().unwrap();
    let _ = std::fs::remove_file(&out);
    let stdout = String::from_utf8_lossy(&run.stdout);
    assert!(
        run.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(stdout.contains("Hello from C"), "{stdout}");
    assert!(stdout.contains(": ok"), "{stdout}");
}
