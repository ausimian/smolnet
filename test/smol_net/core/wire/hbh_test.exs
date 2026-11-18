defmodule SmolNet.Core.Wire.HbhTest do
  # Ported from smoltcp's `wire::ipv6ext_header`, `wire::ipv6option` and
  # `wire::ipv6hbh` unit tests (native/vendor/smoltcp/src/wire/).
  use ExUnit.Case, async: true

  alias SmolNet.Core.Wire.Hbh

  @tcp 6

  describe "ipv6ext_header" do
    # A Hop-by-Hop Option header with a PadN option of option data length 4.
    @repr_packet_pad4 <<0x6, 0x0, 0x1, 0x4, 0x0, 0x0, 0x0, 0x0>>

    # A Hop-by-Hop Option header with a PadN option of option data length 12.
    @repr_packet_pad12 <<0x06, 0x1, 0x1, 0x0C, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
                         0x0, 0x0>>

    test "check_len" do
      # zero byte buffer
      assert Hbh.parse_ext_header(binary_part(@repr_packet_pad4, 0, 0)) == :error
      # no length field
      assert Hbh.parse_ext_header(binary_part(@repr_packet_pad4, 0, 1)) == :error
      # less than 8 bytes
      assert Hbh.parse_ext_header(binary_part(@repr_packet_pad4, 0, 7)) == :error
      # valid
      assert {:ok, _header, _rest} = Hbh.parse_ext_header(@repr_packet_pad4)
      # valid
      assert {:ok, _header, _rest} = Hbh.parse_ext_header(@repr_packet_pad12)
      # length field value greater than number of bytes
      header = <<0x06, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0>>
      assert Hbh.parse_ext_header(header) == :error
    end

    test "header_deconstruct" do
      assert {:ok, {next_header, length, data}, <<>>} = Hbh.parse_ext_header(@repr_packet_pad4)
      assert next_header == @tcp
      assert length == 0
      assert data == binary_part(@repr_packet_pad4, 2, 6)

      assert {:ok, {next_header, length, data}, <<>>} = Hbh.parse_ext_header(@repr_packet_pad12)
      assert next_header == @tcp
      assert length == 1
      assert data == binary_part(@repr_packet_pad12, 2, 14)
    end

    test "overlong" do
      bytes = @repr_packet_pad4 <> <<0>>
      assert {:ok, {_next_header, _length, data}, <<0>>} = Hbh.parse_ext_header(bytes)
      assert byte_size(data) == byte_size(@repr_packet_pad4) - 2

      bytes = @repr_packet_pad12 <> <<0>>
      assert {:ok, {_next_header, _length, data}, <<0>>} = Hbh.parse_ext_header(bytes)
      assert byte_size(data) == byte_size(@repr_packet_pad12) - 2
    end

    test "header_len_overflow" do
      <<next_header, _length, rest::binary>> = @repr_packet_pad4
      len = byte_size(@repr_packet_pad4)
      bytes = <<next_header, len + 1, rest::binary>>
      assert Hbh.parse_ext_header(bytes) == :error

      <<next_header, _length, rest::binary>> = @repr_packet_pad12
      len = byte_size(@repr_packet_pad12)
      bytes = <<next_header, len + 1, rest::binary>>
      assert Hbh.parse_ext_header(bytes) == :error
    end

    test "repr_parse_valid" do
      assert Hbh.parse_ext_header(@repr_packet_pad4) ==
               {:ok, {@tcp, 0, binary_part(@repr_packet_pad4, 2, 6)}, <<>>}

      assert Hbh.parse_ext_header(@repr_packet_pad12) ==
               {:ok, {@tcp, 1, binary_part(@repr_packet_pad12, 2, 14)}, <<>>}
    end

    test "repr_emit" do
      assert Hbh.emit_ext_header(@tcp, 0) == binary_part(@repr_packet_pad4, 0, 2)
      assert Hbh.emit_ext_header(@tcp, 1) == binary_part(@repr_packet_pad12, 0, 2)
    end
  end

  describe "ipv6option" do
    @ipv6option_bytes_pad1 <<0x0>>
    @ipv6option_bytes_padn <<0x1, 0x1, 0x0>>
    @ipv6option_bytes_unknown <<0xFF, 0x3, 0x0, 0x0, 0x0>>
    @ipv6option_bytes_router_alert_mld <<0x05, 0x02, 0x00, 0x00>>
    @ipv6option_bytes_router_alert_rsvp <<0x05, 0x02, 0x00, 0x01>>
    @ipv6option_bytes_router_alert_active_networks <<0x05, 0x02, 0x00, 0x02>>
    @ipv6option_bytes_router_alert_unknown <<0x05, 0x02, 0xBE, 0xEF>>

    test "check_len" do
      # zero byte buffer
      assert Hbh.parse_option(<<>>) == :error
      # pad1
      assert {:ok, _option, _rest} = Hbh.parse_option(@ipv6option_bytes_pad1)

      # padn with truncated data
      assert Hbh.parse_option(binary_part(@ipv6option_bytes_padn, 0, 2)) == :error
      # padn
      assert {:ok, _option, _rest} = Hbh.parse_option(@ipv6option_bytes_padn)

      # router alert with truncated data
      assert Hbh.parse_option(binary_part(@ipv6option_bytes_router_alert_mld, 0, 3)) == :error
      # router alert
      assert {:ok, _option, _rest} = Hbh.parse_option(@ipv6option_bytes_router_alert_mld)

      # unknown option type with truncated data
      assert Hbh.parse_option(binary_part(@ipv6option_bytes_unknown, 0, 4)) == :error
      assert Hbh.parse_option(binary_part(@ipv6option_bytes_unknown, 0, 1)) == :error
      # unknown type
      assert {:ok, _option, _rest} = Hbh.parse_option(@ipv6option_bytes_unknown)
    end

    test "option_deconstruct" do
      # one octet of padding
      assert Hbh.parse_option(@ipv6option_bytes_pad1) == {:ok, :pad1, <<>>}

      # two octets of padding
      assert Hbh.parse_option(<<0x1, 0x0>>) == {:ok, {:padn, 0}, <<>>}

      # three octets of padding
      assert Hbh.parse_option(@ipv6option_bytes_padn) == {:ok, {:padn, 1}, <<>>}

      # extra bytes in buffer
      bytes = <<0x1, 0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xFF>>
      assert Hbh.parse_option(bytes) == {:ok, {:padn, 7}, <<0xFF>>}

      # router alert
      assert Hbh.parse_option(@ipv6option_bytes_router_alert_mld) ==
               {:ok, {:router_alert, 0}, <<>>}

      # unrecognized option without length and data
      assert Hbh.parse_option(<<0xFF>>) == :error
    end

    test "option_parse" do
      # one octet of padding
      assert {:ok, pad1, <<>>} = Hbh.parse_option(@ipv6option_bytes_pad1)
      assert pad1 == :pad1
      assert Hbh.option_buffer_len(pad1) == 1

      # two or more octets of padding
      assert {:ok, padn, <<>>} = Hbh.parse_option(@ipv6option_bytes_padn)
      assert padn == {:padn, 1}
      assert Hbh.option_buffer_len(padn) == 3

      # router alert (MLD)
      assert {:ok, alert, <<>>} = Hbh.parse_option(@ipv6option_bytes_router_alert_mld)
      assert alert == {:router_alert, 0}
      assert Hbh.option_buffer_len(alert) == 4

      # router alert (RSVP)
      assert {:ok, alert, <<>>} = Hbh.parse_option(@ipv6option_bytes_router_alert_rsvp)
      assert alert == {:router_alert, 1}
      assert Hbh.option_buffer_len(alert) == 4

      # router alert (active networks)
      assert {:ok, alert, <<>>} =
               Hbh.parse_option(@ipv6option_bytes_router_alert_active_networks)

      assert alert == {:router_alert, 2}
      assert Hbh.option_buffer_len(alert) == 4

      # router alert (unknown)
      assert {:ok, alert, <<>>} = Hbh.parse_option(@ipv6option_bytes_router_alert_unknown)
      assert alert == {:router_alert, 0xBEEF}
      assert Hbh.option_buffer_len(alert) == 4

      # router alert (incorrect data length)
      assert Hbh.parse_option(<<0x05, 0x03, 0x00, 0x00, 0x00>>) == :error

      # unrecognized option type
      data = <<0, 0, 0>>
      assert {:ok, unknown, <<>>} = Hbh.parse_option(@ipv6option_bytes_unknown)
      assert unknown == {:unknown, 255, 3, data}
    end

    test "option_emit" do
      assert Hbh.emit_option(:pad1) == @ipv6option_bytes_pad1
      assert Hbh.emit_option({:padn, 1}) == @ipv6option_bytes_padn
      assert Hbh.emit_option({:router_alert, 0}) == @ipv6option_bytes_router_alert_mld

      data = <<0, 0, 0>>
      assert Hbh.emit_option({:unknown, 255, 3, data}) == @ipv6option_bytes_unknown
    end

    test "failure_type" do
      # Pad1, PadN and RouterAlert
      assert Hbh.failure_type(0) == :skip
      assert Hbh.failure_type(1) == :skip
      assert Hbh.failure_type(5) == :skip
      assert Hbh.failure_type(0b01000001) == :discard
      assert Hbh.failure_type(0b10100000) == :discard_send_all
      assert Hbh.failure_type(0b11000100) == :discard_send_unicast
    end

    test "options_iter" do
      options =
        <<0x00, 0x01, 0x01, 0x00, 0x01, 0x02, 0x00, 0x00, 0x01, 0x00, 0x00, 0x11, 0x00, 0x05,
          0x02, 0x00, 0x01, 0x01, 0x08, 0x00>>

      assert Hbh.parse_options(options) == [
               {:ok, :pad1},
               {:ok, {:padn, 1}},
               {:ok, {:padn, 2}},
               {:ok, {:padn, 0}},
               {:ok, :pad1},
               {:ok, {:unknown, 0x11, 0, <<>>}},
               {:ok, {:router_alert, 1}},
               :error
             ]
    end
  end

  describe "ipv6hbh" do
    # A Hop-by-Hop Option header with a PadN option of option data length 4.
    @hbh_repr_packet_pad4 <<0x1, 0x4, 0x0, 0x0, 0x0, 0x0>>

    # A Hop-by-Hop Option header with a PadN option of option data length 12.
    @hbh_repr_packet_pad12 <<0x1, 0x0C, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
                             0x0>>

    test "check_len" do
      # zero byte buffer
      assert Hbh.parse(binary_part(@hbh_repr_packet_pad4, 0, 0)) == :error
      # valid
      assert {:ok, _options} = Hbh.parse(@hbh_repr_packet_pad4)
      # valid
      assert {:ok, _options} = Hbh.parse(@hbh_repr_packet_pad12)
    end

    test "repr_parse_valid" do
      assert Hbh.parse(@hbh_repr_packet_pad4) == {:ok, [{:padn, 4}]}
      assert Hbh.parse(@hbh_repr_packet_pad12) == {:ok, [{:padn, 12}]}
    end

    test "repr_emit" do
      options = [{:padn, 4}]
      assert Hbh.buffer_len(options) == 6
      assert Hbh.emit(options) == @hbh_repr_packet_pad4

      options = [{:padn, 12}]
      assert Hbh.buffer_len(options) == 14
      assert Hbh.emit(options) == @hbh_repr_packet_pad12
    end
  end
end
