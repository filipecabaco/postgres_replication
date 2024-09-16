defmodule InspectCallsTest do
  use ExUnit.Case
  doctest InspectCalls

  test "greets the world" do
    assert InspectCalls.hello() == :world
  end
end
