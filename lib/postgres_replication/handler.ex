defmodule PostgresReplication.Handler do
  @type t :: module()
  @doc """
  The `call/2` callback is called by the `PostgresReplication` module to send messages to the parent process. It also sends back to the server connection a message in return if the user wants to.

  ## Parameters
    * `message` - The message to be sent to the parent process.
    * `parent_pid` - The parent process to send the message to.

  ## Returns
    * `{:reply, [term]}` - The message to be sent to server connection. Read more in PostgresReplication.Protocol
    * `:noreply` - No message is sent back to the server.
  """
  @callback call(any, pid()) :: {:reply, [term]} | :noreply
end
