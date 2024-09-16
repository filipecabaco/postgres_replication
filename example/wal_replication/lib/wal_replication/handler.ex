defmodule WalReplication.Handler do
  @behaviour PostgresReplication.Handler
  import PostgresReplication.Protocol
  import PostgresReplication.Decoder
  alias PostgresReplication.Protocol.{Write, KeepAlive}

  @impl true
  def call(message, parent_pid) when is_write(message) do
    %Write{message: message} = parse(message)

    message
    |> decode_message()
    |> then(&send(parent_pid, &1))

    :noreply
  end

  def call(message, _parent_pid) when is_keep_alive(message) do
    %KeepAlive{reply: reply, wal_end: wal_end} = parse(message)

    message =
      case reply do
        :now -> standby(wal_end + 1, wal_end + 1, wal_end + 1, reply)
        :later -> hold()
      end

    {:reply, message}
  end

  # Fallback
  def call(_, _) do
    :no_reply
  end
end
