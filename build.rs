use std::{env, path::Path, process::Command};

fn git(args: &[&str]) -> Option<String> {
    let result = Command::new("git")
        .arg("--no-optional-locks")
        .args(args)
        .output()
        .ok()?;
    result
        .status
        .success()
        .then(|| String::from_utf8(result.stdout).ok())?
}

fn watch_git_path(name: &str) {
    if let Some(path) = git(&["rev-parse", "--git-path", name]) {
        let path = Path::new(path.trim_end());
        if path.exists() {
            println!("cargo::rerun-if-changed={}", path.display());
        } else if name.starts_with("refs/") {
            // A packed branch becomes a loose ref on the next commit.
            if let Some(parent) = path.ancestors().find(|p| p.is_dir()) {
                println!("cargo::rerun-if-changed={}", parent.display());
            }
        }
    }
}

fn utc_date(seconds: i64) -> String {
    // Howard Hinnant's civil_from_days, with March as the first month of a year.
    let days = seconds.div_euclid(86400) + 719468;
    let era = days.div_euclid(146097);
    let day = days.rem_euclid(146097);
    let year = (day - day / 1460 + day / 36524 - day / 146096) / 365;
    let doy = day - (365 * year + year / 4 - year / 100);
    let month = (5 * doy + 2) / 153;
    let dom = doy - (153 * month + 2) / 5 + 1;
    let year = era * 400 + year + i64::from(month >= 10);
    let month = (month + 2) % 12 + 1;
    format!("{year:04}{month:02}{dom:02}")
}

fn git_build() -> Option<String> {
    if git(&["rev-parse", "--is-inside-work-tree"])?.trim() != "true" {
        return None;
    }
    for name in ["HEAD", "index", "packed-refs"] {
        watch_git_path(name);
    }
    if let Some(branch) = git(&["symbolic-ref", "-q", "HEAD"]) {
        watch_git_path(branch.trim_end());
    }
    println!("cargo::rerun-if-changed=src");
    // Include edits to tracked scripts and docs in the dirty flag too.
    for path in git(&["ls-files", "-z"])?.split_terminator('\0') {
        if !path.starts_with("src/") && path != "build.rs" && Path::new(path).exists() {
            println!("cargo::rerun-if-changed={path}");
        }
    }
    let commit = git(&["show", "-s", "--abbrev=7", "--format=%ct %h", "HEAD"])?;
    let (seconds, sha) = commit.trim().split_once(' ')?;
    let date = utc_date(seconds.parse().ok()?);
    let dirty = git(&["status", "--porcelain", "--untracked-files=no"])?;
    Some(format!(
        "{date}.{}{}",
        sha.get(..7)?,
        if dirty.is_empty() { "" } else { ".dirty" }
    ))
}

fn embed_codemode() {
    let out = env::var_os("OUT_DIR").expect("Cargo OUT_DIR");
    let files = ["bootstrap.js", "decoder.js"];
    for file in files {
        println!("cargo::rerun-if-changed=src/codemode/{file}");
    }
    println!("cargo::rerun-if-changed=tools/minify-codemode.mjs");
    if env::var("PROFILE").as_deref() == Ok("release") {
        match Command::new("bun").arg("tools/minify-codemode.mjs").arg(&out).status() {
            Ok(status) if status.success() => return,
            Ok(status) => panic!("Bun could not minify codemode scripts: {status}"),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                println!("cargo::warning=Bun unavailable; embedding original codemode scripts");
            }
            Err(err) => panic!("starting Bun: {err}"),
        }
    }
    for file in files {
        std::fs::copy(Path::new("src/codemode").join(file), Path::new(&out).join(file))
            .expect("copy codemode script");
    }
}

fn main() {
    embed_codemode();
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-env-changed=MYGO_BUILD");
    let mut version = env::var("CARGO_PKG_VERSION").unwrap();
    let build =
        env::var_os("MYGO_BUILD").map_or_else(git_build, |value| {
            let value = value.to_str().expect("MYGO_BUILD must be UTF-8").trim();
            if value.is_empty() {
                return None;
            }
            assert!(
                value.split('.').all(|id| !id.is_empty()
                    && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')),
                "MYGO_BUILD must be valid SemVer build metadata"
            );
            Some(value.to_owned())
        });
    if let Some(build) = build {
        version.push('+');
        version.push_str(&build);
    }
    println!("cargo::rustc-env=MYGO_VERSION={version}");
    if env::var_os("CARGO_CFG_WINDOWS").is_some() {
        // WinSQLite uses stdcall on x86. rusqlite needs the standard C ABI there.
        let library = if env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("x86") {
            "sqlite3"
        } else {
            "winsqlite3"
        };
        println!("cargo::rustc-link-lib=dylib={library}");
    }
}
