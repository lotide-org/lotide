use std::ops::Deref;

struct TestServer {
    host_url: String,
    process: std::process::Child,
}

impl TestServer {
    pub fn start(idx: u16) -> Self {
        let db_url = std::env::var(format!("DATABASE_URL_{}", idx)).expect("Missing DATABASE_URL_#");
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
            process: child
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

    rand::distributions::Alphanumeric.sample_iter(rand::thread_rng()).take(16).collect()
}

#[test]
fn community_fetch() {
    let server1 = TestServer::start(1);
    let server2 = TestServer::start(2);

    let client = reqwest::blocking::Client::builder()
        .build()
        .unwrap();

    let auth = {
        let resp = client.post(format!("{}/api/unstable/users", server1.host_url).deref())
            .json(&serde_json::json!({
                "username": random_string(),
                "password": random_string(),
                "login": true
            }))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap();

        let resp: serde_json::Value = resp.json().unwrap();

        let token = resp["token"].as_str().unwrap();

        format!("Bearer {}", token)
    };

    let community_name = random_string();

    let community_id = {
        let resp = client.post(format!("{}/api/unstable/communities", server1.host_url).deref())
            .header(reqwest::header::AUTHORIZATION, auth)
            .json(&serde_json::json!({
                "name": community_name
            }))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap();

        let resp: serde_json::Value = resp.json().unwrap();

        resp["community"]["id"].as_i64().unwrap()
    };

    let community_id_remote = {
        let resp = client.get(format!("{}/api/unstable/actors:lookup/{}", server2.host_url, percent_encoding::utf8_percent_encode(&format!("{}/apub/communities/{}", server1.host_url, community_id), percent_encoding::NON_ALPHANUMERIC)).deref())
            .send()
            .unwrap()
            .error_for_status()
            .unwrap();

        let resp: (serde_json::Value,) = resp.json().unwrap();
        let (resp,) = resp;
        resp["id"].as_i64().unwrap()
    };

    let resp = client.get(format!("{}/api/unstable/communities/{}", server2.host_url, community_id_remote).deref())
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();
    let resp: serde_json::Value = resp.json().unwrap();

    assert_eq!(resp["name"].as_str(), Some(community_name.as_ref()));
    assert_eq!(resp["local"].as_bool(), Some(false));
}
