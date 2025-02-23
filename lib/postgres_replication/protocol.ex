defmodule PostgresReplication.Protocol do
  @moduledoc """
  Functions for parsing different types of WAL messages.
  """
  alias PostgresReplication.Protocol.KeepAlive
  alias PostgresReplication.Protocol.Write

  defguard is_write(value) when binary_part(value, 0, 1) == <<?w>>
  defguard is_keep_alive(value) when binary_part(value, 0, 1) == <<?k>>

  def parse(
        <<?w, server_wal_start::64, server_wal_end::64, server_system_clock::64, message::binary>>
      ) do
    %Write{
      server_wal_start: server_wal_start,
      server_wal_end: server_wal_end,
      server_system_clock: server_system_clock,
      message: message
    }
  end

  def parse(<<?k, wal_end::64, clock::64, reply::8>>) do
    reply =
      case reply do
        0 -> :later
        1 -> :now
      end

    %KeepAlive{wal_end: wal_end, clock: clock, reply: reply}
  end

  @doc """
  Message to send to the server to request a standby status update.

  Check https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-STANDBY-STATUS-UPDATE for more information
  """
  @spec standby(integer(), integer(), integer(), :now | :later, integer() | nil) :: [binary()]
  def standby(last_wal_received, last_wal_flushed, last_wal_applied, reply, clock \\ nil)

  def standby(last_wal_received, last_wal_flushed, last_wal_applied, reply, nil) do
    standby(last_wal_received, last_wal_flushed, last_wal_applied, reply, current_time())
  end

  def standby(last_wal_received, last_wal_flushed, last_wal_applied, reply, clock) do
    reply =
      case reply do
        :now -> 1
        :later -> 0
      end

    [
      <<?r, last_wal_received::64, last_wal_flushed::64, last_wal_applied::64, clock::64,
        reply::8>>
    ]
  end

  @doc """
  Message to send to ths server to request a hot standby status update.

  https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-HOT-STANDBY-FEEDBACK-MESSAGE
  """
  @spec hot_standby(integer(), integer(), integer(), integer(), integer() | nil) :: [binary()]
  def hot_standby(
        standby_global_xmin,
        standby_global_xmin_epoch,
        standby_catalog_xmin,
        standby_catalog_xmin_epoch,
        clock \\ nil
      )

  def hot_standby(
        standby_global_xmin,
        standby_global_xmin_epoch,
        standby_catalog_xmin,
        standby_catalog_xmin_epoch,
        nil
      ) do
    hot_standby(
      standby_global_xmin,
      standby_global_xmin_epoch,
      standby_catalog_xmin,
      standby_catalog_xmin_epoch,
      current_time()
    )
  end

  def hot_standby(
        standby_global_xmin,
        standby_global_xmin_epoch,
        standby_catalog_xmin,
        standby_catalog_xmin_epoch,
        clock
      ) do
    [
      <<?h, clock::64, standby_global_xmin::32, standby_global_xmin_epoch::32,
        standby_catalog_xmin::32, standby_catalog_xmin_epoch::32>>
    ]
  end

  @doc """
  Message to send the server to not do any operation since the server can wait
  """
  @spec hold :: []
  def hold, do: []

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  def current_time, do: System.os_time(:microsecond) - @epoch
end
