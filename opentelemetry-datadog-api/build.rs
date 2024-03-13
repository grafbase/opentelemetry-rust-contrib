fn main() {
    // Always rerun if the build script itself changes.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=proto/agent_payload.proto");
    println!("cargo:rerun-if-changed=proto/span.proto");
    println!("cargo:rerun-if-changed=proto/stats.proto");
    println!("cargo:rerun-if-changed=proto/tracer_payload.proto");

    let mut prost_build = prost_build::Config::new();
    prost_build.btree_map(["."]);

    tonic_build::configure()
        .compile_with_config(
            prost_build,
            &[
                "proto/agent_payload.proto",
                "proto/span.proto",
                "proto/stats.proto",
                "proto/tracer_payload.proto",
            ],
            &["proto/"],
        )
        .unwrap();
}
