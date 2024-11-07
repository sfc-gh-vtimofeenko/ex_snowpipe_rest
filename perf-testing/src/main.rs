use goose::prelude::*;

use std::env;
use std::fs::read_to_string;
use std::path::Path;
use std::sync::Arc;

use once_cell::sync::Lazy;
//use serde_json::Value;

// Originally this code was written assuming fixture file would be in form:
//
// [
//   { "A": "B", ... },
//   ...
// ]
//
// I.e. a valid JSON document containing an outer array. During the development, fixture file
// format was changed so that every line represents complete payload.
//
// static JSON_DATA: Lazy<Arc<Value>> = Lazy::new(|| {
//     let fixture_path = env::var("FIXTURE_PATH").expect("Cannot parse path");
//     let file_contents = fs::read_to_string(Path::new(&fixture_path)).expect("Cannot read the file");
//     let json = serde_json::from_str(&file_contents).expect("JSON was not well-formatted");
//
//     Arc::new(json) // -> Arc::new(vec!(String))
// });

static FIXTURE_DATA: Lazy<Arc<Vec<String>>> = Lazy::new(|| {
    let fixture_path: String = env::var("FIXTURE_PATH").expect("Cannot parse path");
    let file_lines: Vec<String> = read_to_string(Path::new(&fixture_path))
        .unwrap()
        .lines()
        .map(String::from)
        .collect();

    Arc::new(file_lines)
});

async fn go_brrr(user: &mut GooseUser) -> TransactionResult {
    // Effectively https://docs.rs/goose/latest/src/goose/goose.rs.html#1298
    // But rewritten for PUT

    //let json_data = Arc::clone(&JSON_DATA); // This copies the reference to the data

    let fixture_data = Arc::clone(&FIXTURE_DATA);

    let path = "";

    let reqwest_builder = user
        .client
        .put(user.build_url(path)?)
        .header("Content-Type", "application/json");

    for json_line in fixture_data.iter() {
        let reqwest_builder = reqwest_builder
            .try_clone()
            .expect("Cannot clone the reqwest_builder");
        let goose_request = GooseRequest::builder()
            .method(GooseMethod::Put)
            .path(path)
            .set_request_builder(reqwest_builder.body(json_line.to_string()))
            .build();
        let _goose_metrics = user.request(goose_request).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    GooseAttack::initialize()?
        .register_scenario(
            scenario!("LoadtestTransactions").register_transaction(transaction!(go_brrr)),
        )
        .execute()
        .await?;

    Ok(())
}
