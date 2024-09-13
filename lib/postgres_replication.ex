defmodule PostgresReplication do
  use Postgrex.ReplicationConnection
  require Logger

  @default_opts [
    auto_reconnect: true,
    sync_connect: true
  ]
  @type t :: %__MODULE__{
          connection_opts: Keyword.t(),
          table: String.t(),
          schema: String.t(),
          opts: Keyword.t(),
          step:
            :disconnected
            | :check_replication_slot
            | :create_publication
            | :check_publication
            | :create_slot
            | :start_replication_slot
            | :streaming,
          publication_name: String.t(),
          replication_slot_name: String.t(),
          output_plugin: String.t(),
          proto_version: integer(),
          handler_module: Handler.t(),
          parent_pid: pid()
        }
  defstruct connection_opts: nil,
            table: nil,
            schema: "public",
            opts: [],
            step: :disconnected,
            publication_name: "postgrex_replication",
            replication_slot_name: "postgrex_replication_slot",
            output_plugin: "pgoutput",
            proto_version: 1,
            handler_module: nil,
            parent_pid: nil

  def start_link(%__MODULE__{opts: opts, connection_opts: connection_opts} = attrs) do
    Postgrex.ReplicationConnection.start_link(
      __MODULE__,
      attrs,
      @default_opts |> Keyword.merge(opts) |> Keyword.merge(connection_opts)
    )
  end

  @impl true
  def init(attrs) do
    Logger.info(
      "Initializing connection with the status: #{inspect(attrs |> Map.from_struct() |> Map.drop([:connection_opts]))}"
    )

    {:ok, %{attrs | step: :disconnected}}
  end

  @impl true
  def handle_connect(state) do
    %__MODULE__{replication_slot_name: replication_slot_name} = state
    Logger.info("Checking if replication slot #{replication_slot_name} exists")

    query =
      "SELECT * FROM pg_replication_slots WHERE slot_name = '#{replication_slot_name}'"

    {:query, query, %{state | step: :check_replication_slot}}
  end

  @impl true
  def handle_result(
        [%Postgrex.Result{num_rows: 1}],
        %__MODULE__{step: :check_replication_slot} = state
      ) do
    {:query, "SELECT 1", %{state | step: :create_publication}}
  end

  def handle_result(
        [%Postgrex.Result{num_rows: 0}],
        %__MODULE__{step: :check_replication_slot} = state
      ) do
    %__MODULE__{
      replication_slot_name: replication_slot_name,
      output_plugin: output_plugin,
      step: :check_replication_slot
    } = state

    Logger.info("Create replication slot #{replication_slot_name} using plugin #{output_plugin}")

    query =
      "CREATE_REPLICATION_SLOT #{replication_slot_name} TEMPORARY LOGICAL #{output_plugin} NOEXPORT_SNAPSHOT"

    {:query, query, %{state | step: :create_publication}}
  end

  def handle_result(_, %__MODULE__{step: :check_publication} = state) do
    %__MODULE__{
      publication_name: publication_name,
      table: table,
      schema: schema
    } = state

    Logger.info("Check publication #{publication_name} for table  #{schema}.#{table} exists")
    query = "SELECT * FROM pg_publication WHERE pubname = '#{publication_name}'"

    {:query, query, %{state | step: :create_publication}}
  end

  def handle_result(
        [%Postgrex.Result{num_rows: 0}],
        %__MODULE__{step: :create_publication} = state
      ) do
    %__MODULE__{
      publication_name: publication_name,
      table: table,
      schema: schema
    } = state

    Logger.info("Create publication #{publication_name} for table  #{schema}.#{table}")

    query =
      "CREATE PUBLICATION #{publication_name} FOR TABLE #{schema}.#{table}"

    {:query, query, %{state | step: :create_slot}}
  end

  def handle_result(
        [%Postgrex.Result{num_rows: 1}],
        %__MODULE__{step: :create_publication} = state
      ) do
    query = "SELECT 1"

    {:query, query, %{state | step: :start_replication_slot}}
  end

  @impl true
  def handle_result(_, %__MODULE__{step: :start_replication_slot} = state) do
    %__MODULE__{
      replication_slot_name: replication_slot_name,
      proto_version: proto_version,
      publication_name: publication_name
    } = state

    Logger.info(
      "Starting stream replication for slot #{replication_slot_name} using publication #{publication_name} and protocol version #{proto_version}"
    )

    query =
      "START_REPLICATION SLOT #{replication_slot_name} LOGICAL 0/0 (proto_version '#{proto_version}', publication_names '#{publication_name}')"

    {:stream, query, [], %{state | step: :streaming}}
  end

  @impl true
  def handle_disconnect(state) do
    IO.inspect(state)
    {:noreply, %{state | step: :disconnected}}
  end

  @impl true
  def handle_data(data, state) do
    %__MODULE__{handler_module: handler_module, parent_pid: parent_pid} = state

    case handler_module.call(data, parent_pid) do
      {:reply, messages} -> {:noreply, messages, state}
      :noreply -> {:noreply, state}
    end
  end
end
