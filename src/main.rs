use std::{convert::Infallible, sync::Arc, time::Duration};

use asynk_strim::{Yielder, stream_fn};
use axum::{
    Router,
    extract::State,
    response::{Html, IntoResponse, Sse, sse::Event},
    routing::get,
};
use datastar::patch_elements::PatchElements;
use tokio::{
    sync::{Notify, RwLock},
    time::sleep,
};
use tower_http::compression::{
    CompressionLayer, Predicate as _,
    predicate::{NotForContentType, SizeAbove},
};

struct AppState {
    counter_notify: Notify,
    counter: RwLock<u64>,
}

#[tokio::main]
async fn main() {
    let state = Arc::new(AppState {
        counter_notify: Notify::new(),
        counter: RwLock::new(0),
    });

    let app = Router::new()
        .route("/", get(root))
        .route("/sse", get(sse))
        .layer(
            CompressionLayer::new().br(true).compress_when(
                SizeAbove::default()
                    .and(NotForContentType::GRPC)
                    .and(NotForContentType::IMAGES),
            ),
        )
        .with_state(state.clone());

    tokio::spawn(async move {
        loop {
            *state.counter.write().await += 1;
            state.counter_notify.notify_waiters();
            sleep(Duration::from_secs(1)).await;
        }
    });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> Html<String> {
    r#"

        <script type="module" src="https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.4/bundles/datastar.js"></script>

        <div id="target" data-init="@get('/sse')">target</div>

        "#.to_string().into()
}

async fn sse(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Sse::new(stream_fn(
        move |mut yielder: Yielder<Result<Event, Infallible>>| async move {
            loop {
                state.counter_notify.notified().await;

                let event = PatchElements::new(format!(
                    r#"<div id="target">{}</div>"#,
                    state.counter.read().await
                ));

                yielder
                    .yield_item(Ok(event.write_as_axum_sse_event()))
                    .await;
            }
        },
    ))
}
