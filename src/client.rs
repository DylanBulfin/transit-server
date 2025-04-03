pub mod db_transit {
    tonic::include_proto!("db_transit"); // The string specified here must match the proto package name
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
    // let mut client = TestClient::connect("http://[::1]:50051").await?;
    //
    // let request = tonic::Request::new(HelloRequest {
    //     first_name: "Gin n".into(),
    //     last_name: "Tonic".into(),
    // });
    //
    // let response = client.get_hello(request).await?;
    //
    // println!("RESPONSE={:?}", response);
    //
    // Ok(())
}
