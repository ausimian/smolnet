# The netem matrix: bulk TCP transfers under each netem profile, SmolNet
# sending and receiving through the TUN device, beside the kernel sending
# to itself across a veth impaired the same way, with every transfer's
# length and SHA-256 checked. See SmolNet.Integration.Scenarios.NetemMatrix
# and integration/netem.md for the latest results.
#
#     integration/netem-topology.sh isolate mix run integration/netem_matrix.exs --no-pcap
#
# isolate runs the scenario in a network namespace of its own, with the TUN
# device set up there; without root, in a user namespace, so where the host
# allows those it needs no sudo. `--help` lists every option.

Code.require_file("support/load.exs", __DIR__)

alias SmolNet.Integration.Scenarios.NetemMatrix
alias SmolNet.Integration.Soak

Soak.main(System.argv(), NetemMatrix.config(), &NetemMatrix.run/1)
