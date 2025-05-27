use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tonic_build::compile_protos("proto/db-transit.proto")?;

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let largebuf_copy = out_dir.join("smallbuf");
    let _ = std::fs::create_dir(largebuf_copy.clone()); // This will panic below if the directory failed to create
    tonic_build::configure()
        .out_dir(largebuf_copy)
        .codec_path("crate::common::LargeBufferCodec")
        .compile_protos(&["proto/db-transit.proto"], &["proto"])?;

    Ok(())
}
