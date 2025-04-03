pub mod db_transit {
    tonic::include_proto!("db_transit"); // The string specified here must match the proto package name
}

// #[tonic::async_trait]
// impl test for tester {
//     async fn get_hello(
//         &self,
//         request: request<hellorequest>,
//     ) -> result<response<helloresponse>, status> {
//         println!("received request: {:?}", request);
//
//         let hellorequest {
//             first_name,
//             last_name,
//         } = request.into_inner();
//
//         let response = helloresponse {
//             greeting: format!("hello, {} {}", first_name, last_name),
//         };
//
//         ok(response::new(response))
//     }
// }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
    // let addr = "[::1]:50051".parse()?;
    // let greeter = Tester::default();
    //
    // Server::builder()
    //     .add_service(TestServer::new(greeter))
    //     .serve(addr)
    //     .await?;
    //
    // Ok(())
}
