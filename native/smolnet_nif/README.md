# smolnet_nif

The private Rustler crate for SmolNet. Keep this boundary small: Rust owns the
`smoltcp` stack state and bounded protocol operations; Elixir owns OTP process
lifecycle, deadlines, ownership, framing, and active/passive socket policy.
