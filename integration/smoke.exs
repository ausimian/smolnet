# The smoke scenario: pings SmolNet from the host and echoes TCP payloads
# each way through the TUN device, over IPv4 and IPv6, then checks that the
# stack released every socket. See SmolNet.Integration.Scenarios.Smoke.
#
#     sudo integration/setup.sh
#     sudo mix run integration/smoke.exs [--duration 30s] [--stall]
#
# `--self-check` runs the same workload over the TUN helper's loopback, with
# no device and no root. `--help` lists every option.

Code.require_file("support/load.exs", __DIR__)

alias SmolNet.Integration.Scenarios.Smoke
alias SmolNet.Integration.Soak

Soak.main(System.argv(), Smoke.config(), &Smoke.run/1)
