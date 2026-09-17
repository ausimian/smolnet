defmodule SmolNet.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/ausimian/smolnet"

  def project do
    [
      app: :smolnet,
      version: System.get_env("VERSION_OVERRIDE", @version),
      elixir: ">= 1.18.0 and < 1.21.0",
      start_permanent: Mix.env() == :prod,
      description: "An OTP-friendly smoltcp network stack backed by a small Rust NIF",
      source_url: @source_url,
      homepage_url: @source_url,
      package: package(),
      docs: docs(),
      elixirc_paths: elixirc_paths(Mix.env()),
      test_coverage: [tool: ExCoveralls],
      dialyzer: [
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
        plt_core_path: "priv/plts"
      ],
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {SmolNet.Application, []}
    ]
  end

  def cli do
    [
      preferred_envs: [
        {:precommit, :test},
        {:coveralls, :test},
        {:"coveralls.cobertura", :test},
        {:docs, :test}
      ]
    ]
  end

  defp aliases do
    [
      precommit: [
        "compile --warnings-as-errors",
        "deps.unlock --check-unused",
        "format --check-formatted",
        "credo --strict",
        "dialyzer",
        "docs --warnings-as-errors",
        "cmd cargo fmt --manifest-path native/Cargo.toml --all -- --check",
        "cmd cargo fmt --manifest-path native/fuzz/Cargo.toml --all -- --check",
        "cmd cargo clippy --manifest-path native/Cargo.toml --workspace --all-targets -- -D warnings",
        "cmd cargo test --manifest-path native/Cargo.toml --workspace",
        "cmd cargo check --manifest-path native/fuzz/Cargo.toml --all-targets --locked",
        "run scripts/nif_budget.exs",
        "run examples/quickstart.exs",
        "test --warnings-as-errors"
      ]
    ]
  end

  defp deps do
    [
      {:rustler, "~> 0.38.0", runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:publisho, "~> 1.0", only: :dev, runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: @version,
      source_url: @source_url
    ]
  end

  defp package do
    [
      name: "smolnet",
      licenses: ["MIT"],
      files: [
        "lib",
        "native/Cargo.toml",
        "native/Cargo.lock",
        "native/smolnet_core/Cargo.toml",
        "native/smolnet_core/src",
        "native/smolnet_nif/Cargo.toml",
        "native/smolnet_nif/src",
        "examples/quickstart.exs",
        ".formatter.exs",
        "mix.exs",
        "README.md",
        "CHANGELOG.md",
        "LICENSE"
      ],
      links: %{"GitHub" => @source_url}
    ]
  end
end
