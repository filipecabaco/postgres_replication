# PostgresReplication

Simple wrapper to consume WAL entries from your Postgres Database. It offers an abstraction on top of Postgrex to simplify WAL consumption.

It also offers a decoder in PostgresReplication.Decoder based on [https://github.com/cainophile/cainophile](https://github.com/cainophile/cainophile)

## Usage
Provide the options to connect to the database and the message handler module:

```elixir
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
  def call(_, _), do: :noreply

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time(), do: System.os_time(:microsecond) - @epoch
end

options = %PostgresReplication{
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
  handler_module: PgoutputHandler,
  parent_pid: self()
}

PostgresReplication.start_link(options)

receive do
  msg -> IO.inspect msg
end
```

## Installation

```elixir
def deps do
  [
    {:postgres_replication, "~> 0.1.0"}
  ]
end
```

You need your database to set your `wal_level` to the value of `logical` on start.

```sql
ALTER SYSTEM SET wal_level='logical'
```

