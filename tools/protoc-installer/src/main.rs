fn main() {
    // Building this helper compiles the vendored protoc from protobuf-src.
    // Print the absolute path so callers can export PROTOC before running cargo.
    let protoc_path = protobuf_src::protoc();
    println!("{}", protoc_path.display());
}
