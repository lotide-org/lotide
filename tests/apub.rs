use rstest::*;
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
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(self.process.id() as i32),
            nix::sys::signal::Signal::SIGINT,
        )
        .unwrap();
        self.process.wait().unwrap();
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

#[fixture]
#[once]
fn server1() -> TestServer {
    TestServer::start(1)
}

#[fixture]
#[once]
fn server2() -> TestServer {
    TestServer::start(2)
}

#[rstest]
fn community_fetch(server1: &TestServer, server2: &TestServer) {
    let client = reqwest::blocking::Client::builder().build().unwrap();

    let token = create_account(&client, &server1);

    let community = create_community(&client, &server1, &token);

    let community_remote_id = lookup_community(
        &client,
        &server2,
        &format!("{}/apub/communities/{}", server1.host_url, community.id),
    );

    let resp = client
        .get(
            format!(
                "{}/api/unstable/communities/{}",
                server2.host_url, community_remote_id
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

#[rstest]
fn community_follow(server1: &TestServer, server2: &TestServer) {
    let client = reqwest::blocking::Client::builder().build().unwrap();

    let token1 = create_account(&client, &server1);

    let community = create_community(&client, &server1, &token1);

    let community_remote_id = lookup_community(
        &client,
        &server2,
        &format!("{}/apub/communities/{}", server1.host_url, community.id),
    );

    let token2 = create_account(&client, &server2);

    let resp = client
        .post(
            format!(
                "{}/api/unstable/communities/{}/follow",
                server2.host_url, community_remote_id,
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

#[rstest]
fn community_description_update(server1: &TestServer, server2: &TestServer) {
    let client = reqwest::blocking::Client::builder().build().unwrap();

    let token1 = create_account(&client, &server1);

    let community = create_community(&client, &server1, &token1);

    let community_remote_id = lookup_community(
        &client,
        &server2,
        &format!("{}/apub/communities/{}", server1.host_url, community.id),
    );

    let token2 = create_account(&client, &server2);

    {
        let resp = client
            .post(
                format!(
                    "{}/api/unstable/communities/{}/follow",
                    server2.host_url, community_remote_id,
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
                server1.host_url, community.id
            )
            .deref(),
        )
        .json(&serde_json::json!({ "description_text": new_description }))
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
                    server2.host_url, community_remote_id,
                )
                .deref(),
            )
            .send()
            .unwrap()
            .error_for_status()
            .unwrap();

        let resp: serde_json::Value = resp.json().unwrap();
        assert_eq!(
            resp["description"]["content_html"].as_str(),
            Some(new_description.as_ref())
        );
    }
}
