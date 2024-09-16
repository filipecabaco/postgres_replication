defmodule WalReplication.Worker do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, opts)
  end

  @impl true
  def init(opts) do
    opts = Keyword.delete(opts, :name)
    {:ok, conn} = Postgrex.start_link(opts)
    {:ok, %{pid: conn, operation: nil, relations: %{}}}
  end

  @impl true
  def handle_info(%PostgresReplication.Decoder.Messages.Begin{}, state) do
    {:noreply, state}
  end

  def handle_info(%PostgresReplication.Decoder.Messages.Relation{} = msg, state) do
    %PostgresReplication.Decoder.Messages.Relation{
      id: id,
      namespace: namespace,
      name: name,
      columns: columns
    } =
      msg

    relation = %{namespace: namespace, name: name, columns: columns}
    relations = Map.put(state.relations, id, relation)
    state = Map.put(state, :relations, relations)
    {:noreply, state}
  end

  def handle_info(%PostgresReplication.Decoder.Messages.Insert{} = msg, state) do
    %PostgresReplication.Decoder.Messages.Insert{
      tuple_data: tuple_data,
      relation_id: relation_id
    } = msg

    values =
      for {value, index} <- tuple_data |> Tuple.to_list() |> Enum.with_index() do
        case state.relations
             |> Map.get(relation_id)
             |> then(& &1.columns)
             |> Enum.at(index) do
          %{type: "text", name: name} -> {name, value}
          %{type: "int" <> _, name: name} -> {name, String.to_integer(value)}
        end
      end

    state =
      state
      |> Map.put(:values, values)
      |> Map.put(:relation_id, relation_id)
      |> Map.put(:operation, :insert)

    {:noreply, state}
  end

  def handle_info(%PostgresReplication.Decoder.Messages.Delete{} = msg, state) do
    %PostgresReplication.Decoder.Messages.Delete{
      relation_id: relation_id,
      changed_key_tuple_data: changed_key_tuple_data
    } = msg

    values =
      for {value, index} <- changed_key_tuple_data |> Tuple.to_list() |> Enum.with_index(),
          value != nil do
        case state.relations
             |> Map.get(relation_id)
             |> then(& &1.columns)
             |> Enum.at(index) do
          %{type: "text", name: name} -> {name, value}
          %{type: "int" <> _, name: name} -> {name, String.to_integer(value)}
        end
      end

    state =
      state
      |> Map.put(:values, values)
      |> Map.put(:relation_id, relation_id)
      |> Map.put(:operation, :delete)

    {:noreply, state}
  end

  @impl true
  def handle_info(%PostgresReplication.Decoder.Messages.Update{} = message, state) do
    %PostgresReplication.Decoder.Messages.Update{
      relation_id: relation_id,
      tuple_data: tuple_data
    } = message

    values =
      for {value, index} <- tuple_data |> Tuple.to_list() |> Enum.with_index() do
        case state.relations
             |> Map.get(relation_id)
             |> then(& &1.columns)
             |> Enum.at(index) do
          %{type: "text", name: name} -> {name, value}
          %{type: "int" <> _, name: name} -> {name, String.to_integer(value)}
        end
      end

    state =
      state
      |> Map.put(:values, values)
      |> Map.put(:relation_id, relation_id)
      |> Map.put(:operation, :update)

    {:noreply, state}
  end

  def handle_info(
        %PostgresReplication.Decoder.Messages.Commit{},
        %{pid: pid, relation_id: relation_id} = state
      ) do
    case state.operation do
      :insert ->
        column_names = Enum.map(state.values, &elem(&1, 0))
        values = Enum.map(state.values, &elem(&1, 1))

        arg_index =
          column_names
          |> Enum.with_index(1)
          |> Enum.map_join(",", fn {_, index} -> "$#{index}" end)

        schema = state.relations |> Map.get(relation_id) |> then(& &1.namespace)
        table = state.relations |> Map.get(relation_id) |> then(& &1.name)

        query =
          "INSERT INTO #{schema}.#{table} (#{Enum.join(column_names, ", ")}) VALUES (#{arg_index})"

        Postgrex.query!(pid, query, values)

      :delete ->
        column_names = Enum.map(state.values, &elem(&1, 0))
        values = Enum.map(state.values, &elem(&1, 1))

        conditions =
          column_names
          |> Enum.with_index(1)
          |> Enum.map_join(" AND ", fn {name, index} -> "#{name} = $#{index}" end)

        schema = state.relations |> Map.get(relation_id) |> then(& &1.namespace)
        table = state.relations |> Map.get(relation_id) |> then(& &1.name)

        query =
          "DELETE FROM #{schema}.#{table} WHERE #{conditions}"

        Postgrex.query!(pid, query, values) |> IO.inspect()

      :update ->
        column_names = Enum.map(state.values, &elem(&1, 0))
        values = Enum.map(state.values, &elem(&1, 1))

        set_clause =
          column_names
          |> Enum.with_index(1)
          |> Enum.map_join(", ", fn {name, index} -> "#{name} = $#{index}" end)

        schema = state.relations |> Map.get(relation_id) |> then(& &1.namespace)
        table = state.relations |> Map.get(relation_id) |> then(& &1.name)

        query =
          "UPDATE #{schema}.#{table} SET #{set_clause} WHERE id = $1"

        Postgrex.query!(pid, query, values)

      _ ->
        :ok
    end

    {:noreply, %{pid: pid, relations: state.relations}}
  end
end
