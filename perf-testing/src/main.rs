use goose::prelude::*;

async fn go_brrr(user: &mut GooseUser) -> TransactionResult {
    // Effectively https://docs.rs/goose/latest/src/goose/goose.rs.html#1298
    let json = &serde_json::json!([{
        "a": 1,
        "b": "rust"
    }]);
    let path = "/snowpipe/insert";

    let reqwest_builder = user.client.put(user.build_url(path)?);

    let goose_request = GooseRequest::builder()
        .method(GooseMethod::Put)
        .path(path)
        .set_request_builder(reqwest_builder.json(json))
        .build();

    let _goose_metrics = user.request(goose_request).await?;
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
