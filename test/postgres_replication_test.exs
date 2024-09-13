defmodule PostgresReplicationTest do
  use ExUnit.Case

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
      table: "test",
      opts: [name: __MODULE__, auto_reconnect: true],
      handler_module: PostgresReplicationTest.PgoutputHandler,
      parent_pid: self()
    }

    {:ok, conn} = Postgrex.start_link(opts.connection_opts)
    PostgresReplication.start_link(opts)

    Postgrex.query!(
      conn,
      "INSERT INTO test (column1, column2) VALUES ('Random Text 1', 'Random Text 2')",
      []
    )
    assert_receive %PostgresReplication.Decoder.Messages.Begin{}
    assert_receive %PostgresReplication.Decoder.Messages.Relation{}
    assert_receive %PostgresReplication.Decoder.Messages.Insert{tuple_data: {_, "Random Text 1", "Random Text 2"}}
    assert_receive %PostgresReplication.Decoder.Messages.Commit{}
  end

  defmodule PgoutputHandler do
    @behaviour PostgresReplication.Handler

    @impl true
    def call(<<?w, _header::192, message::binary>>, parent_pid) do
      message |> PostgresReplication.Decoder.decode_message() |> then(&send(parent_pid, &1))
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
    defp current_time(), do: System.os_time(:microsecond) - @epoch
  end
end
