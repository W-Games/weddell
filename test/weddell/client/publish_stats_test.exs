defmodule Weddell.Client.PublishStatsTest do
  use Weddell.UnitCase

  import Mox
  import ExUnit.CaptureLog

  alias GRPC.RPCError
  alias Google.Pubsub.V1.PublishResponse
  alias Weddell.Client

  @project "test-project"
  @topic "test-topic"

  # Mirrors the private stats constants in Weddell.Client.
  @stats_key {Weddell.Client, :publish_stats}
  @published_ix 1
  @failed_ix 2
  @last_report_ix 3
  @stats_interval_ms 10 * 60 * 1000

  describe "publish stats" do
    setup [:setup_client, :reset_stats, :verify_on_exit!]

    test "async publish success counts published messages", %{client: client} do
      expect_publish_ok(1)

      capture_log(fn ->
        assert {:noreply, ^client} =
                 Client.handle_cast({:publish, [{"data-1", %{}}, {"data-2", %{}}], @topic}, client)
      end)

      assert counts() == %{published: 2, failed: 0}
    end

    test "async publish failure counts failed messages", %{client: client} do
      expect_publish_error(1)

      capture_log(fn ->
        assert {:noreply, ^client} =
                 Client.handle_cast({:publish, [{"data-1", %{}}], @topic}, client)
      end)

      assert counts() == %{published: 0, failed: 1}
    end

    test "a single (non-list) message counts as one", %{client: client} do
      expect_publish_ok(1)

      capture_log(fn ->
        Client.handle_cast({:publish, "data", @topic}, client)
      end)

      assert counts() == %{published: 1, failed: 0}
    end

    test "sync publish counts and still returns the result", %{client: client} do
      expect_publish_ok(1)
      expect_publish_error(1)

      capture_log(fn ->
        assert {:reply, :ok, ^client} =
                 Client.handle_call({:publish, [{"a", %{}}], @topic}, from(), client)

        assert {:reply, {:error, %RPCError{}}, ^client} =
                 Client.handle_call({:publish, [{"b", %{}}], @topic}, from(), client)
      end)

      assert counts() == %{published: 1, failed: 1}
    end

    test "stats timestamp is refreshed only after the interval elapses", %{client: client} do
      expect_publish_ok(3)

      # First publish creates the counters and stamps the last-report slot.
      capture_log(fn -> Client.handle_cast({:publish, ["a"], @topic}, client) end)
      stamped = :atomics.get(stats_ref(), @last_report_ix)

      # Within the interval the gate must not fire: stamp unchanged.
      capture_log(fn -> Client.handle_cast({:publish, ["b"], @topic}, client) end)
      assert :atomics.get(stats_ref(), @last_report_ix) == stamped

      # Simulate the interval elapsing: the next publish fires the gate
      # and re-stamps the slot.
      backdated = stamped - @stats_interval_ms
      :atomics.put(stats_ref(), @last_report_ix, backdated)
      capture_log(fn -> Client.handle_cast({:publish, ["c"], @topic}, client) end)
      assert :atomics.get(stats_ref(), @last_report_ix) > backdated
    end
  end

  defp setup_client(_) do
    [client: %Client{project: @project}]
  end

  defp reset_stats(_) do
    :persistent_term.erase(@stats_key)
    on_exit(fn -> :persistent_term.erase(@stats_key) end)
    :ok
  end

  defp expect_publish_ok(n) do
    PublisherStubMock
    |> expect(:publish, n, fn _, _, _ ->
      {:ok, %PublishResponse{message_ids: ["id"]}}
    end)
  end

  defp expect_publish_error(n) do
    PublisherStubMock
    |> expect(:publish, n, fn _, _, _ ->
      {:error, %RPCError{}}
    end)
  end

  defp from do
    {self(), make_ref()}
  end

  defp stats_ref do
    :persistent_term.get(@stats_key)
  end

  defp counts do
    ref = stats_ref()

    %{
      published: :atomics.get(ref, @published_ix),
      failed: :atomics.get(ref, @failed_ix)
    }
  end
end
