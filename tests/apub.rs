use serde_derive::Deserialize;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::Deref;

pub const ACTIVITY_TYPE: &str =
    "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"";

struct TestServer {
    ap_host_url: String,
    real_host_url: String,
    process: std::process::Child,
}

impl TestServer {
    pub fn start(idx: u16) -> Self {
        let db_url =
            std::env::var(format!("DATABASE_URL_{}", idx)).expect("Missing DATABASE_URL_#");
        let ap_host_url = format!("http://{}.lotidetests.localhost", idx);
        let port = portpicker::pick_unused_port().unwrap();
        let real_host_url = format!("http://localhost:{}", port);

        let child = std::process::Command::new(env!("CARGO_BIN_EXE_lotide"))
            .env("DATABASE_URL", db_url)
            .env("PORT", port.to_string())
            .env("HOST_URL_ACTIVITYPUB", format!("{}/apub", ap_host_url))
            .env("HOST_URL_API", format!("{}/api", ap_host_url))
            .env("DEV_MODE", "true")
            .spawn()
            .unwrap();

        let res = Self {
            ap_host_url,
            real_host_url,
            process: child,
        };

        std::thread::sleep(std::time::Duration::from_secs(1));

        res
    }
}

impl std::ops::Drop for TestServer {
    fn drop(&mut self) {
        self.process.kill().unwrap();
    }
}

struct FileInfo {
    content_type: &'static str,
    content: String,
}

struct FileServer {
    url: String,
    file_map: std::sync::Arc<std::sync::RwLock<HashMap<String, FileInfo>>>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl FileServer {
    pub fn start() -> Self {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        let file_map =
            std::sync::Arc::new(std::sync::RwLock::new(HashMap::<String, FileInfo>::new()));

        let listener = std::net::TcpListener::bind(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::LOCALHOST,
            0,
        ))
        .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = {
            let file_map = file_map.clone();
            hyper::Server::from_tcp(listener)
                .unwrap()
                .serve(hyper::service::make_service_fn(move |_| {
                    let file_map = file_map.clone();
                    async move {
                        Result::<_, std::convert::Infallible>::Ok(hyper::service::service_fn({
                            let file_map = file_map.clone();
                            move |req: hyper::Request<hyper::Body>| {
                                let file_map = file_map.clone();
                                async move {
                                    let file_map = file_map.read().unwrap();
                                    if let Some(info) = file_map.get(req.uri().path()) {
                                        let mut res = hyper::Response::new(hyper::Body::from(
                                            info.content.clone(),
                                        ));
                                        res.headers_mut().insert(
                                            hyper::header::CONTENT_TYPE,
                                            info.content_type.try_into().unwrap(),
                                        );

                                        Result::<_, std::convert::Infallible>::Ok(res)
                                    } else {
                                        let mut res =
                                            hyper::Response::new(hyper::Body::from("not found"));
                                        *res.status_mut() = hyper::StatusCode::NOT_FOUND;
                                        Ok(res)
                                    }
                                }
                            }
                        }))
                    }
                }))
        };

        tokio::spawn(async {
            match futures::future::select(server, stop_rx).await {
                futures::future::Either::Left((Err(err), _)) => {
                    eprintln!("Error occurred in test file server: {:?}", err);
                }
                _ => {}
            }
        });

        Self {
            url,
            file_map,
            stop_tx: Some(stop_tx),
        }
    }

    pub fn add_file(&self, path: String, content_type: &'static str, content: String) {
        let file_map = &mut self.file_map.write().unwrap();
        file_map.insert(
            path,
            FileInfo {
                content_type,
                content,
            },
        );
    }
}

impl std::ops::Drop for FileServer {
    fn drop(&mut self) {
        let _ = self.stop_tx.take().unwrap().send(());
    }
}

fn random_string() -> String {
    use rand::distributions::Distribution;

    rand::distributions::Alphanumeric
        .sample_iter(rand::thread_rng())
        .take(16)
        .collect()
}

async fn create_account(client: &reqwest::Client, server: &TestServer) -> String {
    let resp = client
        .post(format!("{}/api/unstable/users", server.real_host_url).deref())
        .json(&serde_json::json!({
            "username": random_string(),
            "password": random_string(),
            "login": true
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    #[derive(Deserialize)]
    struct JustToken {
        token: String,
    }

    let resp: JustToken = resp.json().await.unwrap();

    resp.token
}

struct CommunityInfo {
    id: i64,
    name: String,
}

async fn create_community(
    client: &reqwest::Client,
    server: &TestServer,
    token: &str,
) -> CommunityInfo {
    let community_name = random_string();

    let resp = client
        .post(format!("{}/api/unstable/communities", server.real_host_url).deref())
        .bearer_auth(token)
        .json(&serde_json::json!({ "name": community_name }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp: serde_json::Value = resp.json().await.unwrap();

    CommunityInfo {
        id: resp["community"]["id"].as_i64().unwrap(),
        name: community_name,
    }
}

async fn lookup_community(client: &reqwest::Client, server: &TestServer, ap_id: &str) -> i64 {
    let resp = client
        .get(
            format!(
                "{}/api/unstable/actors:lookup/{}",
                server.real_host_url,
                percent_encoding::utf8_percent_encode(&ap_id, percent_encoding::NON_ALPHANUMERIC)
            )
            .deref(),
        )
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp: (serde_json::Value,) = resp.json().await.unwrap();
    let (resp,) = resp;
    resp["id"].as_i64().unwrap()
}

#[tokio::test]
async fn community_fetch() {
    let server = TestServer::start(1);

    let remote_server = FileServer::start();

    let client = reqwest::Client::builder().build().unwrap();

    let path = format!("/{}", random_string());
    let ap_id = format!("{}{}", remote_server.url, path);
    let name = random_string();
    let content = serde_json::json!({
        "id": ap_id,
        "type": "Group",
        "preferredUsername": name,
        "inbox": format!("{}/inbox", ap_id),
    })
    .to_string();

    remote_server.add_file(path, ACTIVITY_TYPE, content);

    let community_remote_id = lookup_community(&client, &server, &ap_id).await;

    let resp = client
        .get(
            format!(
                "{}/api/unstable/communities/{}",
                server.real_host_url, community_remote_id
            )
            .deref(),
        )
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let resp: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(resp["name"].as_str(), Some(name.as_ref()));
    assert_eq!(resp["local"].as_bool(), Some(false));
}
