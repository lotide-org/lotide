use serde_derive::Deserialize;
use std::ops::Deref;

struct TestServer {
    host_url: String,
    process: std::process::Child,
}

impl TestServer {
    pub fn start(idx: u16) -> Self {
        let db_url =
            std::env::var(format!("DATABASE_URL_{}", idx)).expect("Missing DATABASE_URL_#");
        let port = 8330 + idx;
        let host_url = format!("http://localhost:{}", port);

        let child = std::process::Command::new(env!("CARGO_BIN_EXE_lotide"))
            .env("DATABASE_URL", db_url)
            .env("PORT", port.to_string())
            .env("HOST_URL_ACTIVITYPUB", format!("{}/apub", host_url))
            .env("HOST_URL_API", format!("{}/api", host_url))
            .spawn()
            .unwrap();

        let res = Self {
            host_url,
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

fn random_string() -> String {
    use rand::distributions::Distribution;

    rand::distributions::Alphanumeric
        .sample_iter(rand::thread_rng())
        .take(16)
        .collect()
}

fn create_account(client: &reqwest::blocking::Client, server: &TestServer) -> String {
    let resp = client
        .post(format!("{}/api/unstable/users", server.host_url).deref())
        .json(&serde_json::json!({
            "username": random_string(),
            "password": random_string(),
            "login": true
        }))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();

    #[derive(Deserialize)]
    struct JustToken {
        token: String,
    }

    let resp: JustToken = resp.json().unwrap();

    resp.token
}

struct CommunityInfo {
    id: i64,
    name: String,
}

fn create_community(
    client: &reqwest::blocking::Client,
    server: &TestServer,
    token: &str,
) -> CommunityInfo {
    let community_name = random_string();

    let resp = client
        .post(format!("{}/api/unstable/communities", server.host_url).deref())
        .bearer_auth(token)
        .json(&serde_json::json!({ "name": community_name }))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp: serde_json::Value = resp.json().unwrap();

    CommunityInfo {
        id: resp["community"]["id"].as_i64().unwrap(),
        name: community_name,
    }
}

fn lookup_community(client: &reqwest::blocking::Client, server: &TestServer, ap_id: &str) -> i64 {
    let resp = client
        .get(
            format!(
                "{}/api/unstable/actors:lookup/{}",
                server.host_url,
                percent_encoding::utf8_percent_encode(&ap_id, percent_encoding::NON_ALPHANUMERIC)
            )
            .deref(),
        )
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp: (serde_json::Value,) = resp.json().unwrap();
    let (resp,) = resp;
    resp["id"].as_i64().unwrap()
}

lazy_static::lazy_static! {
    static ref SERVER1: TestServer = TestServer::start(1);
    static ref SERVER2: TestServer = TestServer::start(2);
}

#[test]
fn community_fetch() {
    let client = reqwest::blocking::Client::builder().build().unwrap();

    let token = create_account(&client, &SERVER1);

    let community = create_community(&client, &SERVER1, &token);

    let community_remote_id = lookup_community(
        &client,
        &SERVER2,
        &format!("{}/apub/communities/{}", SERVER1.host_url, community.id),
    );

    let resp = client
        .get(
            format!(
                "{}/api/unstable/communities/{}",
                SERVER2.host_url, community_remote_id
            )
            .deref(),
        )
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();
    let resp: serde_json::Value = resp.json().unwrap();

    assert_eq!(resp["name"].as_str(), Some(community.name.as_ref()));
    assert_eq!(resp["local"].as_bool(), Some(false));
}

#[test]
fn community_follow() {
    let client = reqwest::blocking::Client::builder().build().unwrap();

    let token1 = create_account(&client, &SERVER1);

    let community = create_community(&client, &SERVER1, &token1);

    let community_remote_id = lookup_community(
        &client,
        &SERVER2,
        &format!("{}/apub/communities/{}", SERVER1.host_url, community.id),
    );

    let token2 = create_account(&client, &SERVER2);

    let resp = client
        .post(
            format!(
                "{}/api/unstable/communities/{}/follow",
                SERVER2.host_url, community_remote_id,
            )
            .deref(),
        )
        .json(&serde_json::json!({
            "try_wait_for_accept": true
        }))
        .bearer_auth(token2)
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp: serde_json::Value = resp.json().unwrap();
    assert!(resp["accepted"].as_bool().unwrap());
}

#[test]
fn community_description_update() {
    let client = reqwest::blocking::Client::builder().build().unwrap();

    let token1 = create_account(&client, &SERVER1);

    let community = create_community(&client, &SERVER1, &token1);

    let community_remote_id = lookup_community(
        &client,
        &SERVER2,
        &format!("{}/apub/communities/{}", SERVER1.host_url, community.id),
    );

    let token2 = create_account(&client, &SERVER2);

    {
        let resp = client
            .post(
                format!(
                    "{}/api/unstable/communities/{}/follow",
                    SERVER2.host_url, community_remote_id,
                )
                .deref(),
            )
            .json(&serde_json::json!({
                "try_wait_for_accept": true
            }))
            .bearer_auth(token2)
            .send()
            .unwrap()
            .error_for_status()
            .unwrap();

        let resp: serde_json::Value = resp.json().unwrap();
        assert!(resp["accepted"].as_bool().unwrap());
    }

    let new_description = random_string();

    client
        .patch(
            format!(
                "{}/api/unstable/communities/{}",
                SERVER1.host_url, community.id
            )
            .deref(),
        )
        .json(&serde_json::json!({ "description": new_description }))
        .bearer_auth(token1)
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();

    std::thread::sleep(std::time::Duration::from_secs(1));

    {
        let resp = client
            .get(
                format!(
                    "{}/api/unstable/communities/{}",
                    SERVER2.host_url, community_remote_id,
                )
                .deref(),
            )
            .send()
            .unwrap()
            .error_for_status()
            .unwrap();

        let resp: serde_json::Value = resp.json().unwrap();
        assert_eq!(resp["description"].as_str(), Some(new_description.as_ref()));
    }
}
