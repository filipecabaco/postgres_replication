defmodule Handler do
  @behaviour PostgresReplication.Handler
  import PostgresReplication.Protocol
  alias PostgresReplication.Protocol.KeepAlive

  @impl true
  def call(message, _state) when is_write(message) do
    message
    |> PostgresReplication.Protocol.parse()
    |> PostgresReplication.Decoder.decode_message()
    |> IO.inspect()

    :noreply
  end

  def call(message, _state) when is_keep_alive(message) do
    reply =
      case parse(message) do
        %KeepAlive{reply: :now, wal_end: wal_end} ->
          standby(wal_end + 1, wal_end + 1, wal_end + 1, :now)

        _ ->
          hold()
      end

    {:reply, reply}
  end

  def call(_, _), do: :noreply
end
