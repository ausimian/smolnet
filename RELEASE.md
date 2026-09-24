### Added

- `SmolNet.monitor/1` returns an ordinary monitor on a stack, so a link
  receives a standard `:DOWN` message when its stack stops through
  `SmolNet.stop_stack/1` or a crash, and can exit instead of running its
  transport in front of a stack that is gone.
