use aws_config::BehaviorVersion;
use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use terrapin_dynamo_sdk::{AttributeVal, DynamoClient};
use uuid::Uuid;

const TABLE: &str = "todos";

struct AppState {
    db: DynamoClient,
}

#[derive(Serialize, Deserialize)]
struct Todo {
    id: String,
    title: String,
    done: bool,
}

#[derive(Deserialize)]
struct CreateTodo {
    title: String,
}

#[tokio::main]
async fn main() {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = DynamoClient::new(aws_sdk_dynamodb::Client::new(&config));
    let state = Arc::new(AppState { db: client });

    let app = Router::new()
        .route("/todos", get(list_todos))
        .route("/todos", post(create_todo))
        .route("/todos/:id", get(get_todo))
        .route("/todos/:id/done", post(mark_done))
        .route("/todos/:id", delete(delete_todo))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[flux_rs::source(["/todos"])]
#[flux_rs::sig(fn (State<Arc<AppState>>, Json<CreateTodo>) -> impl std::future::Future<Output = Result<Json<Todo>, StatusCode>>)]
async fn create_todo(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateTodo>,
) -> Result<Json<Todo>, StatusCode> {
    let id = Uuid::new_v4().to_string();

    state
        .db
        .put_item()
        .table_name("todos".to_owned())
        .item("id".to_owned(), AttributeVal::S(id.clone()))
        .item("title".to_owned(), AttributeVal::S(body.title.clone()))
        .item("done".to_owned(), AttributeVal::Bool(false))
        .send()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(Todo {
        id,
        title: body.title,
        done: false,
    }))
}

#[flux_rs::source(["/todos/:id"])]
#[flux_rs::sig(fn (State<Arc<AppState>>, Path<String>) -> impl std::future::Future<Output = Result<Json<Todo>, StatusCode>>)]
async fn get_todo(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Todo>, StatusCode> {
    let table = TABLE;
    let output = state
        .db
        .get_item()
        .table_name(table)
        .key("id", AttributeVal::S(id))
        .send()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let item = output.into_item().ok_or(StatusCode::NOT_FOUND)?;

    let todo = item_to_todo(&item)?;
    Ok(Json(todo))
}

#[flux_rs::source(["/todos"])]
#[flux_rs::sig(fn (State<Arc<AppState>>) -> impl std::future::Future<Output = Result<Json<Todo>, StatusCode>>)]
async fn list_todos(State(state): State<Arc<AppState>>) -> Result<Json<Vec<Todo>>, StatusCode> {
    let table = TABLE;
    let output = state
        .db
        .query()
        .table_name(table)
        .key_condition_expression("done = :done")
        .expression_attribute_values(":done", AttributeVal::Bool(false))
        .send()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let todos = output
        .into_items()
        .iter()
        .filter_map(|item| item_to_todo(item).ok())
        .collect();

    Ok(Json(todos))
}

#[flux_rs::source(["/todos/:id/done"])]
#[flux_rs::sig(fn (State<Arc<AppState>>, Path<String>) -> impl std::future::Future<Output = Result<StatusCode, StatusCode>>)]
async fn mark_done(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let table = TABLE;
    state
        .db
        .update_item()
        .table_name(table)
        .key("id", AttributeVal::S(id))
        .update_expression("SET done = :done")
        .expression_attribute_values(":done", AttributeVal::Bool(true))
        .send()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::NO_CONTENT)
}

#[flux_rs::source(["/todo/:id"])]
#[flux_rs::sig(fn (State<Arc<AppState>>, Path<String>) -> impl std::future::Future<Output = Result<StatusCode, StatusCode>>)]
async fn delete_todo(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let table = TABLE;
    state
        .db
        .delete_item()
        .table_name(table)
        .key("id", AttributeVal::S(id))
        .send()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::NO_CONTENT)
}

fn item_to_todo(item: &terrapin_dynamo_sdk::util::AttributeMap) -> Result<Todo, StatusCode> {
    let id = match item.get("id") {
        Some(AttributeVal::S(s)) => s.clone(),
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };
    let title = match item.get("title") {
        Some(AttributeVal::S(s)) => s.clone(),
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };
    let done = match item.get("done") {
        Some(AttributeVal::Bool(b)) => *b,
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    Ok(Todo { id, title, done })
}
