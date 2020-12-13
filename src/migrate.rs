pub const MIGRATIONS: &[StaticMigration] = include!(concat!(env!("OUT_DIR"), "/migrations.rs"));

pub struct StaticMigration {
    tag: &'static str,
    up: &'static str,
    down: &'static str,
}

pub fn run(mut args: std::env::Args) {
    let action = args.next();
    let action = action.as_deref().unwrap_or("up");

    let database_url = std::env::var("DATABASE_URL").expect("Missing DATABASE_URL");
    let db_cfg: tokio_postgres::Config =
        database_url.parse().expect("Failed to parse DATABASE_URL");

    let mut settings = migrant_lib::Settings::configure_postgres();
    settings
        .database_name(db_cfg.get_dbname().expect("Missing dbname"))
        .database_user(db_cfg.get_user().expect("Missing user"))
        .database_password(
            std::str::from_utf8(db_cfg.get_password().expect("Missing password")).unwrap(),
        );

    let hosts = db_cfg.get_hosts();
    if !hosts.is_empty() {
        let host = hosts
            .iter()
            .filter_map(|host| match host {
                tokio_postgres::config::Host::Tcp(hostname) => Some(hostname),
                _ => None,
            })
            .next();

        match host {
            None => panic!("Unsupported host type"),
            Some(hostname) => {
                settings.database_host(hostname);
            }
        }
    }

    let ports = db_cfg.get_ports();
    if !ports.is_empty() {
        if ports.len() == 1 {
            settings.database_port(ports[0]);
        } else {
            panic!("Multiple ports are not supported");
        }
    }

    let settings = settings.build().unwrap();

    let mut config = migrant_lib::Config::with_settings(&settings);
    config.use_cli_compatible_tags(true);

    if action == "setup" {
        config.setup().expect("Failed to setup database");
    } else {
        let migrations: Vec<_> = MIGRATIONS
            .iter()
            .map(|item| {
                migrant_lib::EmbeddedMigration::with_tag(item.tag)
                    .up(item.up)
                    .down(item.down)
                    .boxed()
            })
            .collect();
        config
            .use_migrations(&migrations)
            .expect("Failed to initialize migrations");

        let config = config.reload().expect("Failed to check status");

        match action {
            "up" => {
                println!("Applying migrations...");
                migrant_lib::Migrator::with_config(&config)
                    .all(true)
                    .swallow_completion(true)
                    .apply()
                    .expect("Failed to apply migrations");
            }
            "down" => {
                println!("Unapplying migration...");
                migrant_lib::Migrator::with_config(&config)
                    .direction(migrant_lib::Direction::Down)
                    .all(false)
                    .swallow_completion(true)
                    .apply()
                    .expect("Failed to undo migration");
            }
            _ => panic!("Unknown migrate action"),
        }
    }
}
