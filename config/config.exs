import Config

config :smolnet, SmolNet.Native, mode: if(config_env() == :prod, do: :release, else: :debug)
