fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
        // WinSQLite uses stdcall on x86. rusqlite needs the standard C ABI there.
        let library = if std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("x86") {
            "sqlite3"
        } else {
            "winsqlite3"
        };
        println!("cargo::rustc-link-lib=dylib={library}");
    }
}
