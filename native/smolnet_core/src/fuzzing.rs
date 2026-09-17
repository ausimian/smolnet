use crate::stack::{fuzz_config_and_endpoints, fuzz_raw_engine, fuzz_socket_lifecycle};

pub fn raw_packet(data: &[u8]) {
    fuzz_raw_engine(data);
}

pub fn endpoint_options(data: &[u8]) {
    fuzz_config_and_endpoints(data);
}

pub fn socket_operations(data: &[u8]) {
    fuzz_socket_lifecycle(data);
}
