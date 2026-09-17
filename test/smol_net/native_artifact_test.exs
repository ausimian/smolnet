defmodule SmolNet.NativeArtifactTest do
  use ExUnit.Case, async: true

  alias SmolNet.NativeArtifact

  describe "verify_entries/2" do
    test "accepts exactly the expected regular NIF" do
      assert :ok =
               NativeArtifact.verify_entries(
                 [{"libsmolnet_nif-v0.1.0-nif-2.15-x86_64-unknown-linux-gnu.so", :regular}],
                 "libsmolnet_nif-v0.1.0-nif-2.15-x86_64-unknown-linux-gnu.so"
               )
    end

    test "rejects traversal, absolute paths, and unexpected files" do
      expected = "libsmolnet_nif.so"

      for name <- ["../../libsmolnet_nif.so", "/tmp/libsmolnet_nif.so", "payload"] do
        assert {:error, message} = NativeArtifact.verify_entries([{name, :regular}], expected)
        assert message =~ "exactly one regular entry"
      end
    end

    test "rejects links, devices, directories, and duplicate entries" do
      expected = "libsmolnet_nif.so"

      for type <- [:symlink, :link, :device, :directory] do
        assert {:error, _message} = NativeArtifact.verify_entries([{expected, type}], expected)
      end

      assert {:error, _message} =
               NativeArtifact.verify_entries(
                 [{expected, :regular}, {expected, :regular}],
                 expected
               )
    end
  end

  test "pinned_checksum!/2 accepts only a lowercase SHA-256 entry for the asset" do
    path = Path.join(System.tmp_dir!(), "smolnet-checksum-#{System.unique_integer([:positive])}")
    checksum = String.duplicate("a", 64)
    File.write!(path, inspect(%{"asset.tar.gz" => "sha256:#{checksum}"}))

    on_exit(fn -> File.rm(path) end)

    assert NativeArtifact.pinned_checksum!(path, "asset.tar.gz") == checksum

    assert_raise ArgumentError, ~r/no pinned checksum/, fn ->
      NativeArtifact.pinned_checksum!(path, "other.tar.gz")
    end
  end
end
