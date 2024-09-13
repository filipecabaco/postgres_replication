defmodule PostgresReplication.Handler do
  @type t :: module()
  @callback call(any, pid()) :: {:reply, [term]} | :noreply
end
