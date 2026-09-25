defmodule SmolNet.MixProject do
  use Mix.Project

  @version "0.4.2"
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
      compilers: compilers(),
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
      "compile.smolnet_nif": &prepare_precompiled_nif/1,
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
        "cmd cargo test --manifest-path native/vendor/smoltcp/Cargo.toml --lib --target-dir native/target/vendor",
        "cmd cargo check --manifest-path native/fuzz/Cargo.toml --all-targets --locked",
        "run scripts/nif_budget.exs",
        "run examples/quickstart.exs",
        "run examples/loopback.exs",
        "test --warnings-as-errors"
      ]
    ]
  end

  defp deps do
    [
      {:rustler, "~> 0.38.0", optional: true, runtime: false},
      {:rustler_precompiled, "~> 0.9", runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:publisho, "~> 1.0", only: :dev, runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  # Repository checkouts retain the Rust workspace and compile through
  # Rustler. Hex packages omit it, so consumers validate and cache the
  # matching precompiled archive before RustlerPrecompiled extracts it.
  defp compilers do
    if File.dir?("native/smolnet_nif") do
      Mix.compilers()
    else
      [:smolnet_nif | Mix.compilers()]
    end
  end

  defp docs do
    guides = [
      "gen_tcp.md",
      "gen_udp.md",
      "socket_api.md"
    ]

    [
      main: "readme",
      extras: ["README.md" | guides] ++ ["CHANGELOG.md"],
      groups_for_extras: [Guides: guides, Releases: ["CHANGELOG.md"]],
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
        "checksum-*.exs",
        "examples/quickstart.exs",
        "examples/loopback.exs",
        "gen_tcp.md",
        "gen_udp.md",
        "socket_api.md",
        ".formatter.exs",
        "mix.exs",
        "README.md",
        "MAINTAINING.md",
        "CHANGELOG.md",
        "LICENSE"
      ],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp prepare_precompiled_nif(_args) do
    with_native_artifact(fn ->
      metadata = nif_metadata!()
      asset = metadata.file_name
      cache_path = metadata.cached_tar_gz
      checksum_path = Path.expand("checksum-Elixir.SmolNet.Native.exs", __DIR__)
      expected = SmolNet.NativeArtifact.pinned_checksum!(checksum_path, asset)
      expected_entry = metadata.lib_name <> ".so"

      if valid_cached_archive?(cache_path, expected, expected_entry) do
        Mix.shell().info("Using verified precompiled NIF #{asset} from cache")
      else
        File.mkdir_p!(Path.dirname(cache_path))
        temporary = cache_path <> ".download-#{System.unique_integer([:positive])}"

        try do
          Mix.shell().info("Downloading precompiled NIF #{asset}")
          http_download!("#{@source_url}/releases/download/#{@version}/#{asset}", temporary)
          SmolNet.NativeArtifact.verify_checksum!(temporary, expected)
          SmolNet.NativeArtifact.verify_archive!(temporary, expected_entry)
          File.rm(cache_path)
          File.rename!(temporary, cache_path)
        after
          File.rm(temporary)
        end
      end
    end)

    {:ok, []}
  end

  defp nif_metadata! do
    config =
      RustlerPrecompiled.Config.new(
        otp_app: :smolnet,
        module: SmolNet.Native,
        crate: "smolnet_nif",
        base_url: "#{@source_url}/releases/download/#{@version}",
        version: @version,
        force_build: false,
        targets: ~w(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-apple-darwin),
        nif_versions: ["2.15"]
      )

    case RustlerPrecompiled.build_metadata(config) do
      {:ok, metadata} ->
        metadata

      {:error, reason} ->
        Mix.raise(
          "#{reason}\nSmolNet Hex packages provide GNU/Linux x86_64/AArch64 and " <>
            "Apple Silicon macOS NIFs only. " <>
            "Use a source checkout with Rust 1.91 or newer for other targets."
        )
    end
  end

  defp valid_cached_archive?(path, expected, expected_entry) do
    File.exists?(path) and
      SmolNet.NativeArtifact.checksum_valid?(path, expected) and
      match?(:ok, SmolNet.NativeArtifact.verify_archive(path, expected_entry))
  end

  # Mix prunes much of the parent VM's code path while compiling a dependency.
  # A peer node has a complete OTP code path for verified HTTPS downloads.
  defp http_download!(url, destination) do
    {:ok, peer, _node} =
      :peer.start_link(%{connection: :standard_io, name: :peer.random_name()})

    try do
      {:ok, _} = :peer.call(peer, :application, :ensure_all_started, [:inets])
      {:ok, _} = :peer.call(peer, :application, :ensure_all_started, [:ssl])

      cacerts = :peer.call(peer, :public_key, :cacerts_get, [])
      match_fun = :peer.call(peer, :public_key, :pkix_verify_hostname_match_fun, [:https])

      http_options = [
        autoredirect: true,
        ssl: [
          verify: :verify_peer,
          cacerts: cacerts,
          customize_hostname_check: [match_fun: match_fun]
        ]
      ]

      request = {String.to_charlist(url), []}
      options = [body_format: :binary, stream: String.to_charlist(destination)]

      case :peer.call(
             peer,
             :httpc,
             :request,
             [:get, request, http_options, options],
             :infinity
           ) do
        {:ok, :saved_to_file} ->
          :ok

        {:ok, {{_, 200, _}, _headers, _body}} ->
          :ok

        {:ok, {{_, status, reason}, _headers, _body}} ->
          Mix.raise("NIF download failed (HTTP #{status} #{reason}): #{url}")

        {:error, reason} ->
          Mix.raise("NIF download failed (#{inspect(reason)}): #{url}")
      end
    after
      :peer.stop(peer)
    end
  end

  defp with_native_artifact(fun) do
    preloaded? = Code.ensure_loaded?(SmolNet.NativeArtifact)

    unless preloaded? do
      Code.require_file("lib/smol_net/native_artifact.ex", __DIR__)
    end

    try do
      fun.()
    after
      unless preloaded? do
        :code.purge(SmolNet.NativeArtifact)
        :code.delete(SmolNet.NativeArtifact)
      end
    end
  end
end
