defmodule WalReplicationTest do
  use ExUnit.Case
  doctest WalReplication

  test "greets the world" do
    assert WalReplication.hello() == :world
  end
end
