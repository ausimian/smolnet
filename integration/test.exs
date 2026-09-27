# Runs the integration harness's own tests: its pure pieces, and runs over
# the TUN helper's loopback. Needs no device and no root:
#
#     mix run integration/test.exs
#
# These are not part of `mix test`, which never loads the harness.

Code.require_file("support/load.exs", __DIR__)

ExUnit.start(autorun: false)

__DIR__
|> Path.join("test/**/*_test.exs")
|> Path.wildcard()
|> Enum.sort()
|> Enum.each(&Code.require_file/1)

%{failures: failures} = ExUnit.run()
System.halt(if failures == 0, do: 0, else: 1)
