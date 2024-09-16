defmodule WalReplication do
  use Application

  @db1 [
    hostname: "localhost",
    username: "postgres",
    password: "postgres",
    database: "postgres",
    port: 5432,
    parameters: [
      application_name: "PostgresReplication"
    ]
  ]

  @db2 [
    hostname: "localhost",
    username: "postgres",
    password: "postgres",
    database: "postgres",
    port: 5433,
    parameters: [
      application_name: "PostgresReplication"
    ]
  ]

  def start(_type, _args) do
    {:ok, _} =
      Supervisor.start_link([{WalReplication.Worker, @db2 ++ [name: WalReplication.Worker]}],
        strategy: :one_for_one,
        name: WalReplication.Worker.Supervisor
      )

    children = [
      {PostgresReplication,
       %PostgresReplication{
         connection_opts: [
           hostname: "localhost",
           username: "postgres",
           password: "postgres",
           database: "postgres",
           port: 5432,
           parameters: [
             application_name: "PostgresReplication"
           ]
         ],
         table: :all,
         opts: [name: __MODULE__, auto_reconnect: true],
         handler_module: WalReplication.Handler,
         parent_pid: Process.whereis(WalReplication.Worker)
       }}
    ]

    opts = [strategy: :one_for_one, name: WalReplication.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def db1(), do: Postgrex.start_link([name: WalReplication.DB1] ++ @db1)
  def db2(), do: Postgrex.start_link([name: WalReplication.DB2] ++ @db2)

  def run_insert() do
    db1()
    query = "INSERT INTO random_values (value) VALUES ('Random Text 1') RETURNING id"
    %{rows: [[id]]} = Postgrex.query!(Process.whereis(WalReplication.DB1), query, [])
    id
  end

  def run_delete() do
    id = run_insert()
    query = "DELETE FROM random_values WHERE id = #{id}"
    Postgrex.query!(Process.whereis(WalReplication.DB1), query, [])
  end

  def run_update() do
    id = run_insert()
    query = "UPDATE random_values SET value = 'Random Text 2' WHERE id = #{id}"
    Postgrex.query!(Process.whereis(WalReplication.DB1), query, [])
  end
end
