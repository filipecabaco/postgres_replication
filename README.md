# PostgresReplication

Simple wrapper to consume WAL entries from your Postgres Database. It offers an abstraction on top of Postgrex to simplify WAL consumption.

It also offers a decoder in PostgresReplication.Decoder based on [https://github.com/cainophile/cainophile](https://github.com/cainophile/cainophile)

## Usage
Provide the options to connect to the database and the message handler module. As an example present in the [examples](./example/) folder here's how you can track events on all tables:

```elixir
Mix.install([{:postgres_replication, git: "https://github.com/filipecabaco/postgres_replication.git"}])
defmodule Handler do
  @behaviour PostgresReplication.Handler
  import PostgresReplication.Protocol
  alias PostgresReplication.Protocol.KeepAlive

  @impl true
  def call(message, _parent_pid) when is_write(message) do
    message
    |> PostgresReplication.Protocol.parse()
    |> PostgresReplication.Decoder.decode_message()
    |> IO.inspect()

    :noreply
  end

  def call(message, _parent_pid) when is_keep_alive(message) do
    reply =
      case parse(message) do
        %KeepAlive{reply: :now, wal_end: wal_end} ->
          wal_end = wal_end + 1
          standby(wal_end, wal_end, wal_end, :now)

        _ ->
          hold()
      end

    {:reply, reply}
  end

  def call(_, _), do: :noreply
end


options = %PostgresReplication{
  connection_opts: [
    hostname: "localhost",
    username: "postgres",
    password: "postgres",
    database: "postgres",
  ],
  table: :all,
  opts: [name: __MODULE__, auto_reconnect: true],
  handler_module: Handler,
}

PostgresReplication.start_link(options)
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

