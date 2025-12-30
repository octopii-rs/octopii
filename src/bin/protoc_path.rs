fn main() {
    let path = protoc_bin_vendored::protoc_bin_path()
        .expect("failed to resolve vendored protoc");
    println!("{}", path.display());
}
