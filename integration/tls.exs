# The TLS scenario: bulk HTTPS downloads and uploads over :ssl on SmolNet,
# single- and multi-stream, over IPv4 and IPv6, with every body's length and
# SHA-256 checked and any TLS alert a failure. See
# SmolNet.Integration.Scenarios.Tls.
#
#     sudo integration/setup.sh
#     sudo mix run integration/tls.exs [--duration 6h] [--target local]
#
# The default target is speed.cloudflare.com, reached through the host's NAT,
# with each phase repeated over the kernel's stack for comparison;
# `--target local` serves the transfers from this host instead. `--baseline`
# runs the workload over the kernel alone, and `--self-check --target local`
# over the TUN helper's loopback, with no device and no root. `--help` lists
# every option.

Code.require_file("support/load.exs", __DIR__)

alias SmolNet.Integration.Scenarios.Tls
alias SmolNet.Integration.Soak

Soak.main(System.argv(), Tls.config(), &Tls.run/1)
