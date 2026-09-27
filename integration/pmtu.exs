# The path MTU scenario: TCP and UDP each way between SmolNet and a kernel
# peer across a hop whose MTU is below SmolNet's, over IPv4 and IPv6, with
# and without the hop's ICMP errors and MSS clamping, and a verdict on each
# case: adapts, stalls or fails. See SmolNet.Integration.Scenarios.Pmtu.
#
#     integration/pmtu-topology.sh isolate mix run integration/pmtu.exs [--mtu 1280]
#
# isolate runs the scenario in a network namespace of its own, with the TUN
# device set up there; without root, in a user namespace, so where the host
# allows those it needs no sudo. `--help` lists every option.

Code.require_file("support/load.exs", __DIR__)

alias SmolNet.Integration.Scenarios.Pmtu
alias SmolNet.Integration.Soak

Soak.main(System.argv(), Pmtu.config(), &Pmtu.run/1)
