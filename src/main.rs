use std::{convert::Infallible, time::Duration};

use asynk_strim::{Yielder, stream_fn};
use axum::{
    Router,
    response::{Html, IntoResponse, Sse, sse::Event},
    routing::get,
};
use datastar::patch_elements::PatchElements;
use tokio::time::sleep;
use tower_http::compression::{
    CompressionLayer, Predicate as _,
    predicate::{NotForContentType, SizeAbove},
};

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(root))
        .route("/sse", get(sse))
        .layer(
            CompressionLayer::new().br(true).compress_when(
                SizeAbove::default()
                    .and(NotForContentType::GRPC)
                    .and(NotForContentType::IMAGES),
            ),
        );

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> Html<String> {
    r#"

        <script type="module" src="https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.4/bundles/datastar.js"></script>

        <div id="target" data-init="@get('/sse')">target</div>

        "#.to_string().into()
}

async fn sse() -> impl IntoResponse {
    Sse::new(stream_fn(
        move |mut yielder: Yielder<Result<Event, Infallible>>| async move {
            loop {
                let event = PatchElements::new(
                    r#"

        <div id="target">

        xxxxxxxxxx
        xxxxxxxxxx
        xxxxxxxxxx
        xxxxxxxxxx
        xxxxxxxxxx
        xxxxxxxxxx
        xxxxxxxxxx
        xxxxxxxxxx
        xxxxxxxxxx
        xxxxxxxxxx

        </div>

                    "#,
                );

                yielder
                    .yield_item(Ok(event.write_as_axum_sse_event()))
                    .await;

                sleep(Duration::from_secs(1)).await;
            }
        },
    ))
}
