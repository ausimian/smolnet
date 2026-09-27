# The idle scenario: holds hundreds of TCP and TLS connections open to echo
# servers on the host, mostly idle, trickling or bursting after long idles,
# while a share of them lose their peer silently, and measures how soon
# SmolNet notices and how busy its timers are while idle. See
# SmolNet.Integration.Scenarios.Idle.
#
#     sudo integration/setup.sh --no-nat
#     sudo mix run integration/idle.exs [--duration 6h] [--idle-max 2h]
#
# Peers vanish through nftables, so that part needs root. `--baseline` runs
# the same over the kernel's loopback, for comparison, and `--self-check`
# the working connections over the TUN helper's loopback, with no device and
# no root. `--help` lists every option.

Code.require_file("support/load.exs", __DIR__)

alias SmolNet.Integration.Scenarios.Idle
alias SmolNet.Integration.Soak

Soak.main(System.argv(), Idle.config(), &Idle.run/1)
