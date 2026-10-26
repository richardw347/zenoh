// Regression for liveliness resource and mapping reclamation.
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0

use crate::net::routing::dispatcher::resource::Resource;
use crate::sample::SampleKind;
use crate::{Config, Session};
use std::time::Duration;
use zenoh_config::WhatAmI;

async fn session(mode: WhatAmI, endpoint: &str) -> Session {
    let mut config = Config::default();
    config.set_mode(Some(mode)).unwrap();
    config.scouting.multicast.set_enabled(Some(false)).unwrap();
    config.scouting.gossip.set_enabled(Some(false)).unwrap();
    let endpoint = endpoint.parse().unwrap();
    if mode == WhatAmI::Router {
        config.listen.endpoints.set(vec![endpoint]).unwrap();
    } else {
        config.listen.endpoints.set(vec![]).unwrap();
        config
            .connect
            .endpoints
            .set(vec![zenoh_config::EndPoints::Single(endpoint)])
            .unwrap();
    }
    crate::open(config).await.unwrap()
}

fn retained_names(session: &Session, repetitions: usize) -> (usize, usize) {
    let gateway = session.static_runtime().unwrap().router();
    let tables = gateway.tables.tables.read().unwrap();
    let resources = (0..repetitions)
        .filter(|i| {
            Resource::get_resource(&tables.data.root_res, &format!("memory/churn/{i}")).is_some()
        })
        .count();
    let mappings = tables
        .data
        .faces
        .values()
        .map(|face| {
            face.local_mappings
                .values()
                .chain(face.remote_mappings.values())
                .filter(|res| {
                    let name = res.expr();
                    name.starts_with("memory/churn/") && name != "memory/churn/**"
                })
                .count()
        })
        .sum();
    (resources, mappings)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleted_liveliness_names_release_resources_and_mappings() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("tcp/127.0.0.1:{}", listener.local_addr().unwrap().port());
    drop(listener);
    let router = session(WhatAmI::Router, &endpoint).await;
    let producer = session(WhatAmI::Client, &endpoint).await;
    let observer = session(WhatAmI::Client, &endpoint).await;
    // Existing tokens exercise current/history replies, separately from the
    // future updates below. The ordering of history delivery is unspecified.
    let mut existing = Vec::new();
    for i in 0..16 {
        existing.push(
            producer
                .liveliness()
                .declare_token(format!("memory/churn/{i}"))
                .await
                .unwrap(),
        );
    }
    let samples = observer
        .liveliness()
        .declare_subscriber("memory/churn/**")
        .history(true)
        .await
        .unwrap();
    let mut expected: std::collections::HashSet<_> =
        (0..16).map(|i| format!("memory/churn/{i}")).collect();
    for _ in 0..16 {
        let put = tokio::time::timeout(Duration::from_secs(3), samples.recv_async())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(put.kind(), SampleKind::Put);
        assert!(
            expected.remove(put.key_expr().as_str()),
            "unexpected history token"
        );
    }
    assert!(expected.is_empty());
    for (i, token) in existing.into_iter().enumerate() {
        token.undeclare().await.unwrap();
        let delete = tokio::time::timeout(Duration::from_secs(3), samples.recv_async())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delete.kind(), SampleKind::Delete);
        assert_eq!(delete.key_expr().as_str(), format!("memory/churn/{i}"));
    }
    let repetitions = 256;
    for i in 16..repetitions {
        let name = format!("memory/churn/{i}");
        let token = producer
            .liveliness()
            .declare_token(name.clone())
            .await
            .unwrap();
        let put = tokio::time::timeout(Duration::from_secs(3), samples.recv_async())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(put.kind(), SampleKind::Put);
        assert_eq!(put.key_expr().as_str(), name);
        token.undeclare().await.unwrap();
        let delete = tokio::time::timeout(Duration::from_secs(3), samples.recv_async())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delete.kind(), SampleKind::Delete);
        assert_eq!(delete.key_expr().as_str(), name);
    }
    // Observer delivery may precede the router's final resource cleanup.
    tokio::time::sleep(Duration::from_millis(100)).await;
    for (name, session) in [
        ("router", &router),
        ("producer", &producer),
        ("observer", &observer),
    ] {
        let retained = retained_names(session, repetitions);
        eprintln!(
            "{name}: retained resources={}, mappings={}",
            retained.0, retained.1
        );
        assert_eq!(
            retained,
            (0, 0),
            "deleted liveliness names retained by {name}"
        );
    }
    drop(samples);
    for session in [observer, producer, router] {
        tokio::time::timeout(Duration::from_secs(5), session.close())
            .await
            .unwrap()
            .unwrap();
    }
}
