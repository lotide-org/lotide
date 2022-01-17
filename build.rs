use std::collections::HashSet;
use std::io::Write;

const MIGRATIONS_DIR: &str = "migrations";
const DEFAULT_LANG_FILE: &str = "res/lang/en.ftl";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR")?;

    {
        println!("cargo:rerun-if-changed={}", MIGRATIONS_DIR);

        let mut out_file =
            std::fs::File::create(std::path::Path::new(&out_dir).join("migrations.rs"))?;
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
    }
    {
        println!("cargo:rerun-if-changed={}", DEFAULT_LANG_FILE);

        let mut out_file =
            std::fs::File::create(std::path::Path::new(&out_dir).join("lang_keys.rs"))?;

        let content = std::fs::read_to_string(DEFAULT_LANG_FILE)?;
        let ast = match fluent_syntax::parser::parse_runtime(content.as_ref()) {
            Ok(ast) => ast,
            Err((_, errors)) => {
                panic!("Failed to load default lang file: {:?}", errors);
            }
        };

        for entry in ast.body {
            if let fluent_syntax::ast::Entry::Message(msg) = entry {
                let id = msg.id.name;
                let mut args: Vec<&str> = Vec::new();

                println!("finding arguments for {:?}", msg.value);

                if let Some(value) = msg.value {
                    discover_args_for_pattern(&mut args, &value);
                }

                let args: Vec<_> = {
                    let mut set = HashSet::new();
                    args.into_iter().filter(|key| set.insert(*key)).collect()
                };

                if args.is_empty() {
                    writeln!(
                        out_file,
                        "pub const fn {0}() -> PlainLangKey {{ PlainLangKey(\"{0}\") }}",
                        id
                    )?;
                } else {
                    write!(out_file, "pub fn {}<'a>(", id)?;

                    {
                        let mut first = true;
                        for arg in &args {
                            if !first {
                                write!(out_file, ", ")?;
                            }
                            first = false;

                            write!(out_file, "{}: impl Into<fluent::FluentValue<'a>>", arg)?;
                        }
                    }

                    writeln!(out_file, ") -> LangKeyWithArgs<'a> {{")?;

                    write!(
                        out_file,
                        "LangKeyWithArgs(\"{}\", fluent::fluent_args![",
                        id
                    )?;
                    {
                        let mut first = true;
                        for arg in args {
                            if !first {
                                write!(out_file, ", ")?;
                            }
                            first = false;

                            write!(out_file, "\"{0}\" => {0}", arg)?;
                        }
                    }
                    writeln!(out_file, "])")?;
                    writeln!(out_file, "}}")?;
                }
            }
        }
    }

    Ok(())
}

fn discover_args_for_pattern<'a>(
    target: &mut Vec<&'a str>,
    pattern: &fluent_syntax::ast::Pattern<&'a str>,
) {
    for elem in &pattern.elements {
        if let fluent_syntax::ast::PatternElement::Placeable { expression } = elem {
            discover_args_for_expression(target, expression);
        }
    }
}

fn discover_args_for_expression<'a>(
    target: &mut Vec<&'a str>,
    expr: &fluent_syntax::ast::Expression<&'a str>,
) {
    match expr {
        fluent_syntax::ast::Expression::Select { selector, variants } => {
            discover_args_for_inline_expression(target, selector);

            for variant in variants {
                discover_args_for_pattern(target, &variant.value);
            }
        }
        fluent_syntax::ast::Expression::Inline(expr) => {
            discover_args_for_inline_expression(target, expr)
        }
    }
}

fn discover_args_for_inline_expression<'a>(
    target: &mut Vec<&'a str>,
    expr: &fluent_syntax::ast::InlineExpression<&'a str>,
) {
    use fluent_syntax::ast::InlineExpression;

    match expr {
        InlineExpression::StringLiteral { .. }
        | InlineExpression::NumberLiteral { .. }
        | InlineExpression::MessageReference { .. }
        | InlineExpression::TermReference {
            arguments: None, ..
        } => {}
        InlineExpression::FunctionReference { arguments, .. }
        | InlineExpression::TermReference {
            arguments: Some(arguments),
            ..
        } => {
            for arg in &arguments.positional {
                discover_args_for_inline_expression(target, arg);
            }
            for arg in &arguments.named {
                discover_args_for_inline_expression(target, &arg.value);
            }
        }
        InlineExpression::Placeable { expression } => {
            discover_args_for_expression(target, expression);
        }
        InlineExpression::VariableReference { id } => {
            target.push(id.name);
        }
    }
}
