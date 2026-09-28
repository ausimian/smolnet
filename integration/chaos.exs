# The chaos scenario: link, device, credit and process faults injected into
# a stack carrying TLS bulk transfers, connection churn and blocked calls,
# each episode checked for hangs, orphans, missed DOWNs and wrong errors,
# and a fresh stack checked after it. See SmolNet.Integration.Scenarios.Chaos.
#
#     integration/pmtu-topology.sh isolate mix run integration/chaos.exs [--seed N]
#
# isolate runs it in a network namespace of its own, with the TUN device set
# up there (setup.sh --no-nat), so that its device and route faults touch
# nothing else; without root, in a user namespace. `--self-check` runs it
# over the helper's loopback, without the device faults. `--help` lists
# every option.

Code.require_file("support/load.exs", __DIR__)

alias SmolNet.Integration.Scenarios.Chaos
alias SmolNet.Integration.Soak

Soak.main(System.argv(), Chaos.config(), &Chaos.run/1)
