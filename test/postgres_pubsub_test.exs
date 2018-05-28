defmodule PGTest do
  use ExUnit.Case
  doctest PG

  test "greets the world" do
    assert PG.hello() == :world
  end
end
