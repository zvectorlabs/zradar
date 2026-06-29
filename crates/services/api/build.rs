fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto");

    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);

    // Query service protos
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("zradar_query_v1_descriptor.bin"))
        .compile_protos(
            &[proto_dir.join("zradar/query/v1/query.proto")],
            std::slice::from_ref(&proto_dir),
        )?;

    // Admin service protos
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("zradar_admin_v1_descriptor.bin"))
        .compile_protos(
            &[proto_dir.join("zradar/admin/v1/admin.proto")],
            std::slice::from_ref(&proto_dir),
        )?;

    Ok(())
}
