# The crawl scenario: TLS `HEAD /` to the Tranco top 1,000 sites, over
# SmolNet and over the kernel, host by host, for as long as the run lasts,
# with SmolNet's socket ceiling pushed past against a listener on this host
# first. Hosts that fail over SmolNet but not the kernel are listed, with
# evidence cut from the capture. See SmolNet.Integration.Scenarios.Crawl.
#
#     sudo integration/setup.sh
#     sudo mix run integration/crawl.exs [--duration 1h] [--top 10000]
#
# The list is fetched from tranco-list.eu when the run starts and saved with
# its artifacts. `--target local` crawls servers on this host instead, and
# `--list fixture` a small committed list. `--help` lists every option.

Code.require_file("support/load.exs", __DIR__)

alias SmolNet.Integration.Scenarios.Crawl
alias SmolNet.Integration.Soak

Soak.main(System.argv(), Crawl.config(), &Crawl.run/1)
