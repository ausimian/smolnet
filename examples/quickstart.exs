address = {0xFD00, 0, 0, 0, 0, 0, 0, 1}

{:ok, stack} =
  SmolNet.start_stack(
    egress: {self(), :quickstart},
    addresses: [{address, 64}]
  )

{:ok, info} = SmolNet.stack_info(stack)
:running = info.native.result.lifecycle
0 = info.native.result.socket_count
:ok = SmolNet.stop_stack(stack)
