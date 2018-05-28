defmodule PG do
  def start do
    {:ok, pid} =
      Postgrex.Notifications.start_link(
        hostname: "localhost",
        username: "postgres",
        password: "postgres",
        database: "test"
      )
  end
end
