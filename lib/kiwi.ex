defmodule Kiwi do
  @moduledoc """
  A simple key value store with persistent storage based on ets and postgresql
  """

  # TODO: setup a GenStage consumer to push out events and have multiple connections
  # to update the ets table? Is that insane?

  defmodule EtsUpdater do
    use GenServer
    def start_link(), do: GenServer.start_link(__MODULE__, :noarg)

    def init(_args) do
      {:ok, pid} =
        Postgrex.Notifications.start_link(
          hostname: "localhost",
          username: "postgres",
          password: "postgres",
          database: "kiwi"
        )

      # TODO: use a pool of connections
      {:ok, data_conn} =
        Postgrex.start_link(
          hostname: "localhost",
          username: "postgres",
          password: "postgres",
          database: "kiwi"
        )

      {:ok, ref} = Postgrex.Notifications.listen(pid, "kiwi_update")

      {:ok, {pid, ref, data_conn}}
    end

    def handle_info(
          {:notification, _pid, _ref, "kiwi_update", notification},
          {_, _, data_conn} = state
        ) do
      IO.puts("GOT NOTIFICATION")

      case notification do
        "INSERT" <> key ->
          fetch_key(data_conn, key)

        "DELETE" <> key ->
          delete_key(key)

        "UPDATE" <> key ->
          fetch_key(data_conn, key)
      end

      {:noreply, state}
    end

    defp fetch_key(conn, key) do
      IO.puts("Updating keys: #{key}")
      # TODO: use a named query
      case Postgrex.query(conn, "SELECT val FROM kiwi WHERE key = $1", [key]) do
        {:ok, %Postgrex.Result{num_rows: 1, rows: [[val]]}} ->
          :ets.insert(:kiwi, {key, :erlang.binary_to_term(val)})

        {:ok, %Postgrex.Result{num_rows: 0}} ->
          # row was deleted and will be handled by the next delete notification
          :ok
      end
    end

    defp delete_key(key) do
      :ets.delete(:kiwi, key)
    end
  end

  # TODO: use a connection pool instead of passing explicit connections
  def start_link() do
    # TODO: move ets to a supervised process, so that a crash in this connection
    # does not have to reload the whole ets table
    spawn(fn ->
      :kiwi =
        :ets.new(:kiwi, [
          :named_table,
          :public,
          :set,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

      Process.sleep(:infinity)
    end)

    Postgrex.start_link(
      hostname: "localhost",
      username: "postgres",
      password: "postgres",
      database: "kiwi"
    )
  end

  # This needs the following table to be created in the database
  # CREATE TABLE kiwi(key VARCHAR(255) PRIMARY KEY, val BYTEA);

  def set(conn, key, value) when is_binary(key) do
    # write to the database and let the notify propogate to update ets
    # TODO: use a named query
    Postgrex.query(conn, "INSERT INTO kiwi VALUES($1, $2)", [
      key,
      :erlang.term_to_binary(value)
    ])
  end
end
