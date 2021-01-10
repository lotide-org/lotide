use std::io::Write;

const MIGRATIONS_DIR: &str = "migrations";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed={}", MIGRATIONS_DIR);

    let out_dir = std::env::var("OUT_DIR")?;
    let mut out_file = std::fs::File::create(std::path::Path::new(&out_dir).join("migrations.rs"))?;
    writeln!(out_file, "&[")?;

    let paths: Result<Vec<_>, _> = std::fs::read_dir(MIGRATIONS_DIR)?.collect();
    let mut paths = paths?;

    paths.sort_by_cached_key(|entry| entry.file_name());

    for entry in paths {
        let filename = entry.file_name();
        let tag = filename.to_str().unwrap();

        let path = entry.path().canonicalize()?;
        let path = path.to_str().unwrap();

        writeln!(
            out_file,
            r##"StaticMigration {{ tag: r#"{}"#, up: include_str!(r#"{1}{2}up.sql"#), down: include_str!(r#"{1}{2}down.sql"#) }},"##,
            tag,
            path,
            std::path::MAIN_SEPARATOR
        )?;
    }

    write!(out_file, "]")?;

    Ok(())
}
