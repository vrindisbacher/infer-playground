use dynamo::{AttributeVal, DynamoClient, PutItemBuilder};

fn main() {
    // let _ = simple_inline_example(&MyClient {});
}

// #[flux_rs::source]
// #[flux_rs::sig(fn () -> ())]
// fn foo() {
//     let mut keys = Keys::new();
//     keys = keys.add("hello");
//     bar(keys);
// }

// fn b() -> bool {
//     todo!()
// }

// fn bar(mut keys: Keys) {
//     // if b() {
//     keys = keys.add("world");
//     baz(keys);
//     // } else {
//     //     keys = keys.add("baz");
//     //     baz(keys);
//     // }
// }

// #[flux_rs::sink]
// fn baz(keys: Keys) {
//     assert(10 < 11);
// }

// #[flux_rs::opaque]
// #[flux_rs::refined_by(keys: Set<str>)]
// pub struct Keys;

// impl Keys {
//     #[flux_rs::trusted]
//     #[flux_rs::sig(fn () -> Self[set_empty(0)])]
//     pub fn new() -> Self {
//         Self {}
//     }

//     #[flux_rs::trusted]
//     #[flux_rs::sig(fn (Self[@keys], &str[@k]) -> Self[{ keys: set_union(keys, set_singleton(k)) }])]
//     pub fn add(self, key: &str) -> Self {
//         Self {}
//     }
// }

#[flux_rs::source]
#[flux_rs::sig(fn (&DynamoClient, noise: usize) -> impl std::future::Future<Output = Result<(), ()>>)]
async fn complex_calling_example(client: &DynamoClient, noise: usize) -> Result<(), ()> {
    let dynamo_call = client.put_item().table_name("my-first_table".to_string());
    foo(dynamo_call, noise).await;

    // keep doing work
    let mut second_dynamo_call = client.put_item().table_name("my-second-table".to_string());
    second_dynamo_call =
        second_dynamo_call.item("1".to_string(), AttributeVal::s("HELLO".to_string()));
    let _ = second_dynamo_call.send().await;

    Ok(())
}

fn baz(noise: usize) -> bool {
    noise > 99
}

async fn foo(mut dynamo_req: PutItemBuilder, noise: usize) {
    dynamo_req = dynamo_req.item("2".to_string(), AttributeVal::s("HELLO".to_string()));
    if baz(noise) {
        bar(dynamo_req).await;
    } else {
        let _ = dynamo_req.send().await;
    }
}

async fn bar(dynamo_req: PutItemBuilder) {
    let _ = dynamo_req
        .item("3".to_string(), AttributeVal::s("hello".to_string()))
        .send()
        .await;
}

#[flux_rs::sig(fn (b: bool) requires b)]
fn assert(b: bool) {}

#[flux_rs::source]
#[flux_rs::sig(fn (&DynamoClient) -> impl std::future::Future<Output = Result<(), ()>>)]
async fn simple_inline_example(client: &DynamoClient) -> Result<(), ()> {
    let _ = client
        .put_item()
        .table_name("wawatable".to_string())
        .item("1".to_string(), AttributeVal::s("HELLO".to_string()))
        .item("2".to_string(), AttributeVal::s("PROFILE".to_string()))
        .item("3".to_string(), AttributeVal::s("WORLD".to_string()))
        .send()
        .await;
    Ok(())
}

// {
//     table_name: 1,
//     item_map: Map({
//         1: "HELLO",
//         2: "PROFILE",
//         ...
//     })
// }
