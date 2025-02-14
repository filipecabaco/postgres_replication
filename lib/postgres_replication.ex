defmodule PostgresReplication do
  @moduledoc """
  PostgresReplication is a module that provides a way to stream data from a PostgreSQL database using logical replication.

  ## Struct parameters
  * `connection_opts` - The connection options to connect to the database.
  * `table` - The table to replicate. If `:all` is passed, it will replicate all tables.
  * `schema` - The schema of the table to replicate. If not provided, it will use the `public` schema. If `:all` is passed, this option is ignored.
  * `opts` - The options to pass to this module
  * `step` - The current step of the replication process
  * `publication_name` - The name of the publication to create. If not provided, it will use the schema and table name.
  * `replication_slot_name` - The name of the replication slot to create. If not provided, it will use the schema and table name.
  * `output_plugin` - The output plugin to use. Default is `pgoutput`.
  * `output_plugin_options` - The options to pass to the output plugin If you use :publication_name as the value of the key, the lib will automatically set the generated or set value of `publication_name`. Default is [proto_version: "1", publication_names: :publication_name].
  * `handler_module` - The module that will handle the data received from the replication stream.
  * `target_pid` - The PID of the parent process that will receive the data.

  """
  use Postgrex.ReplicationConnection
  require Logger
  alias PostgresReplication.Handler

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
          output_plugin_options: Keyword.t(),
          handler_module: Handler.t(),
          metadata: map()
        }
  defstruct connection_opts: nil,
            table: nil,
            schema: "public",
            opts: [],
            step: :disconnected,
            publication_name: nil,
            replication_slot_name: nil,
            output_plugin: "pgoutput",
            output_plugin_options: [proto_version: "1", publication_names: :publication_name],
            handler_module: nil,
            metadata: %{}

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

    publication_name = publication_name(attrs)
    replication_slot_name = replication_slot_name(attrs)

    {:ok,
     %{
       attrs
       | step: :disconnected,
         publication_name: publication_name,
         replication_slot_name: replication_slot_name
     }}
  end

  @impl true
  def handle_connect(state) do
    replication_slot_name = replication_slot_name(state)
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
    {:query, "SELECT 1", %{state | step: :check_publication}}
  end

  def handle_result(
        [%Postgrex.Result{num_rows: 0}],
        %__MODULE__{step: :check_replication_slot} = state
      ) do
    %__MODULE__{
      output_plugin: output_plugin,
      replication_slot_name: replication_slot_name,
      step: :check_replication_slot
    } = state

    Logger.info("Create replication slot #{replication_slot_name} using plugin #{output_plugin}")

    query =
      "CREATE_REPLICATION_SLOT #{replication_slot_name} TEMPORARY LOGICAL #{output_plugin} NOEXPORT_SNAPSHOT"

    {:query, query, %{state | step: :check_publication}}
  end

  def handle_result(
        [%Postgrex.Result{}],
        %__MODULE__{step: :check_publication} = state
      ) do
    %__MODULE__{table: table, schema: schema, publication_name: publication_name} = state

    Logger.info("Check publication #{publication_name} for table #{schema}.#{table} exists")
    query = "SELECT * FROM pg_publication WHERE pubname = '#{publication_name}'"

    {:query, query, %{state | step: :create_publication}}
  end

  def handle_result(
        [%Postgrex.Result{num_rows: 0}],
        %__MODULE__{step: :create_publication, table: :all} = state
      ) do
    %{publication_name: publication_name} = state
    Logger.info("Create publication #{publication_name} for all tables")

    query =
      "CREATE PUBLICATION #{publication_name} FOR ALL TABLES"

    {:query, query, %{state | step: :start_replication_slot}}
  end

  def handle_result(
        [%Postgrex.Result{num_rows: 0}],
        %__MODULE__{step: :create_publication} = state
      ) do
    %__MODULE__{
      table: table,
      schema: schema,
      publication_name: publication_name
    } = state

    Logger.info("Create publication #{publication_name} for table #{schema}.#{table}")

    query =
      "CREATE PUBLICATION #{publication_name} FOR TABLE #{schema}.#{table}"

    {:query, query, %{state | step: :start_replication_slot}}
  end

  def handle_result(
        [%Postgrex.Result{num_rows: 1}],
        %__MODULE__{step: :create_publication} = state
      ) do
    {:query, "SELECT 1", %{state | step: :start_replication_slot}}
  end

  @impl true
  def handle_result(
        [%Postgrex.Result{}],
        %__MODULE__{step: :start_replication_slot} = state
      ) do
    %__MODULE__{
      replication_slot_name: replication_slot_name,
      publication_name: publication_name,
      output_plugin_options: output_plugin_options
    } = state

    Logger.info(
      "Starting stream replication for slot #{replication_slot_name} using plugins options: #{inspect(output_plugin_options)}"
    )

    output_plugin_options =
      output_plugin_options
      |> Enum.map_join(", ", fn
        {k, :publication_name} -> "#{k} '#{publication_name}'"
        {k, v} -> "#{k} '#{v}'"
      end)
      |> String.trim()

    query =
      "START_REPLICATION SLOT #{replication_slot_name} LOGICAL 0/0 (#{output_plugin_options})"

    {:stream, query, [], %{state | step: :streaming}}
  end

  @impl true
  def handle_disconnect(state) do
    Logger.error(
      "Disconnected from the server: #{inspect(state |> Map.from_struct() |> Map.drop([:connection_opts]))}"
    )

    {:noreply, %{state | step: :disconnected}}
  end

  @impl true
  def handle_data(data, state) do
    %__MODULE__{handler_module: handler_module} = state

    case handler_module.call(data, state) do
      {:reply, messages} -> {:noreply, messages, state}
      :noreply -> {:noreply, state}
    end
  end

  def publication_name(%__MODULE__{publication_name: nil, table: :all}) do
    "all_table_publication"
  end

  def publication_name(%__MODULE__{publication_name: nil, table: table, schema: schema}) do
    "#{schema}_#{table}_publication"
  end

  def publication_name(%__MODULE__{publication_name: publication_name}) do
    publication_name
  end

  def replication_slot_name(%__MODULE__{replication_slot_name: nil, table: :all}) do
    "all_table_slot"
  end

  def replication_slot_name(%__MODULE__{replication_slot_name: nil, table: table, schema: schema}) do
    "#{schema}_#{table}_replication_slot"
  end

  def replication_slot_name(%__MODULE__{replication_slot_name: replication_slot_name}) do
    replication_slot_name
  end
end
