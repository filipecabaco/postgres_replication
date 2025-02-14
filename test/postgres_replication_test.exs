defmodule PostgresReplicationTest do
  use ExUnit.Case
  alias PostgresReplication.Plugin.Pgoutput.Decoder

  test "handles database connection and receives WAL changes" do
    opts = %PostgresReplication{
      connection_opts: [
        hostname: "localhost",
        username: "postgres",
        password: "postgres",
        database: "postgres",
        parameters: [
          application_name: "PostgresReplication"
        ]
      ],
      table: :all,
      opts: [name: __MODULE__],
      handler_module: PostgresReplicationTest.PgoutputHandler,
      metadata: %{pid: self()}
    }

    {:ok, conn} = Postgrex.start_link(opts.connection_opts)
    PostgresReplication.start_link(opts)

    Postgrex.query!(
      conn,
      "INSERT INTO random_values (value) VALUES ('Random Text 1')",
      []
    )

    assert_receive %Decoder.Messages.Begin{}
    assert_receive %Decoder.Messages.Relation{}

    assert_receive %Decoder.Messages.Insert{
      tuple_data: {_, "Random Text 1"}
    }

    assert_receive %Decoder.Messages.Commit{}
  end

  test "handles database connection, receives WAL changes and you can set plugin options" do
    opts = %PostgresReplication{
      connection_opts: [
        hostname: "localhost",
        username: "postgres",
        password: "postgres",
        database: "postgres",
        parameters: [
          application_name: "PostgresReplication"
        ]
      ],
      table: :all,
      opts: [name: __MODULE__],
      publication_name: "test_publication",
      output_plugin: "pgoutput",
      output_plugin_options: [
        proto_version: "2",
        publication_names: :publication_name
      ],
      handler_module: PostgresReplicationTest.PgoutputHandler,
      metadata: %{pid: self()}
    }

    {:ok, conn} = Postgrex.start_link(opts.connection_opts)
    PostgresReplication.start_link(opts)

    Postgrex.query!(
      conn,
      "INSERT INTO random_values (value) VALUES ('Random Text 1')",
      []
    )

    assert_receive %Decoder.Messages.Begin{}
    assert_receive %Decoder.Messages.Relation{}

    assert_receive %Decoder.Messages.Insert{
      tuple_data: {_, "Random Text 1"}
    }

    assert_receive %Decoder.Messages.Commit{}
  end

  test "handles database connection, receives WAL changes and can use plugins without options" do
    opts = %PostgresReplication{
      connection_opts: [
        hostname: "localhost",
        username: "postgres",
        password: "postgres",
        database: "postgres",
        parameters: [
          application_name: "PostgresReplication"
        ]
      ],
      table: :all,
      opts: [name: __MODULE__],
      publication_name: "test_publication",
      output_plugin: "test_decoding",
      output_plugin_options: [],
      handler_module: PostgresReplicationTest.TestDecodingHandler,
      metadata: %{pid: self()}
    }

    {:ok, conn} = Postgrex.start_link(opts.connection_opts)
    PostgresReplication.start_link(opts)

    Postgrex.query!(
      conn,
      "INSERT INTO random_values (value) VALUES ('Random Text 1')",
      []
    )

    assert_receive :ok
    assert_receive :ok
    assert_receive :ok
  end

  defmodule TestDecodingHandler do
    @behaviour PostgresReplication.Handler

    @impl true
    def call(<<?w, _header::192, message::binary>>, %{metadata: %{pid: pid}}) do
      send(pid, :ok)
      :noreply
    end

    # Handles keep alive messages
    def call(<<?k, wal_end::64, _clock::64, reply>>, _) do
      messages =
        case reply do
          1 -> [<<?r, wal_end + 1::64, wal_end + 1::64, wal_end + 1::64, current_time()::64, 0>>]
          0 -> []
        end

      {:reply, messages}
    end

    @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
    defp current_time, do: System.os_time(:microsecond) - @epoch
  end

  defmodule PgoutputHandler do
    @behaviour PostgresReplication.Handler

    @impl true
    def call(<<?w, _header::192, message::binary>>, %{metadata: %{pid: pid}}) do
      message |> Decoder.decode_message() |> then(&send(pid, &1))
      :noreply
    end

    # Handles keep alive messages
    def call(<<?k, wal_end::64, _clock::64, reply>>, _) do
      messages =
        case reply do
          1 -> [<<?r, wal_end + 1::64, wal_end + 1::64, wal_end + 1::64, current_time()::64, 0>>]
          0 -> []
        end

      {:reply, messages}
    end

    @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
    defp current_time, do: System.os_time(:microsecond) - @epoch
  end
end
