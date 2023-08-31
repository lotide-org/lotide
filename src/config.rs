use serde_derive::Deserialize;
use std::collections::HashMap;

fn default_port() -> u16 {
    3333
}

#[derive(Deserialize)]
pub struct Config {
    pub database_url: String,
    pub database_certificate_path: Option<std::path::PathBuf>,

    pub host_url_activitypub: String,
    pub host_url_api: String,

    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default)]
    pub apub_proxy_rewrites: bool,
    #[serde(default)]
    pub allow_forwarded: bool,
    #[serde(default)]
    pub dev_mode: bool,

    pub media_storage: Option<String>,
    pub media_location: Option<String>,

    pub media_s3_region: Option<String>,
    pub media_s3_endpoint: Option<String>,
    pub media_s3_access_key_id: Option<String>,
    pub media_s3_secret_key: Option<String>,

    pub smtp_url: Option<String>,
    pub smtp_from: Option<String>,

    #[serde(default)]
    pub break_stuff: bool,

    #[serde(default)]
    pub debug_stuck: bool,
}

impl Config {
    pub fn load(config_file_path: Option<&std::ffi::OsStr>) -> Result<Self, config::ConfigError> {
        let mut src = config::Config::new()
            .with_merged(config::Environment::new())?
            .with_merged(config::Environment::with_prefix("LOTIDE"))?;

        if let Some(config_file_path) = config_file_path {
            src.merge(SpecificFile {
                path: config_file_path.into(),
            })?;
        }

        src.try_into()
    }
}

#[derive(Debug, Clone)]
struct SpecificFile {
    path: std::path::PathBuf,
}

impl config::Source for SpecificFile {
    fn clone_into_box(&self) -> Box<dyn config::Source + Send + Sync> {
        Box::new(self.clone())
    }

    fn collect(&self) -> Result<HashMap<String, config::Value>, config::ConfigError> {
        let uri = self.path.to_string_lossy().into_owned();

        let content = match std::fs::read_to_string(&self.path) {
            Ok(content) => content,
            Err(cause) => {
                return Err(config::ConfigError::FileParse {
                    uri: Some(uri),
                    cause: Box::new(cause),
                })
            }
        };

        config::FileFormat::Ini
            .parse(Some(&uri), &content)
            .map_err(|cause| config::ConfigError::FileParse {
                uri: Some(uri),
                cause,
            })
    }
}
