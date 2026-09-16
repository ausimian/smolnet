# smolnet_nif

The private Rustler crate for SmolNet. Keep this boundary small: Rust owns the
`smoltcp` stack state and bounded protocol operations; Elixir owns OTP process
lifecycle, deadlines, ownership, framing, and active/passive socket policy.

TCP stream calls copy at most the configured byte limit and fixed socket-buffer
capacity. Rust retains neither unsent payload remainders nor exact-receive
accumulators. Established close state is retained only for bounded FIN driving
and is removed by bounded maintenance sweeps.
