defmodule Kelvin.InOrderSubscription do
  @moduledoc """
  A subscription producer which processes events in order as they appear
  in the EventStoreDB

  ## Options

  * `:name` - (optional) the GenServer name for this producer
  * `:stream_name` - (required) the stream name to which to subscribe
  * `:connection` - (required) the Extreme client module to use as a
    connection to the EventStoreDB. This may either be the name of the
    Extreme client module or its pid.
  * `:restore_stream_position!` - (required) a function which determines
    the stream position from which this listener should begin after initializing
    or restarting. Values may be either an MFA tuple or a 0-arity anonymous
    function.
  * `:subscribe_on_init?` - (required) a function which determines whether
    the producer should subscribe immediately after starting up. Values may
    be either an MFA tuple or a 0-arity anonymous function. The function
    should return either `true` to subscribe immediately on initialization or
    `false` if the author intends on manually subscribing the producer. This
    producer can be manually subscribed by `send/2`ing a message of
    `:subscribe` to the process.
  * `:subscribe_after` - (default: `Enum.random(3_000..5_000)`) the amount of
    time to wait after initializing to query the `:subscribe_on_init?` option.
    This can be useful to prevent all producers from trying to subscribe at
    the same time and to await an active connection to the EventStoreDB.
  * `:catch_up_chunk_size` - (default: `256`) the number of events to query
    for each read chunk while catching up. This option presents a trade-off
    between network queries and query duration over the network.
  """

  use GenStage
  require Logger

  defstruct [
    :config,
    :extreme_listener,
    :self,
    :max_buffer_size,
    demand: 0,
    buffer: :queue.new(),
    buffer_size: 0
  ]

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @impl GenStage
  def init(opts) do
    max_buffer_size =
      Keyword.get(
        opts,
        :catch_up_chunk_size,
        Application.get_env(:kelvin, :catch_up_chunk_size, 256)
      )

    connection = Keyword.fetch!(opts, :connection)
    stream_name = Keyword.fetch!(opts, :stream_name)

    listener_name =
      opts
      |> Keyword.get(:name, __MODULE__)
      |> Module.concat(ExtremeListener)

    {:ok, extreme_listener} =
      Kelvin.Listener.start_link(connection, stream_name,
        read_per_page: max_buffer_size,
        auto_subscribe: false,
        ack_timeout: :infinity,
        name: listener_name,
        producer: self(),
        get_stream_position_fun: fn ->
          opts
          |> Keyword.fetch!(:restore_stream_position!)
          |> _do_function()
        end
      )

    state = %__MODULE__{
      extreme_listener: extreme_listener,
      config: Map.new(opts),
      self: Keyword.get(opts, :name, self()),
      max_buffer_size: max_buffer_size * 2
    }

    Process.send_after(
      self(),
      :check_auto_subscribe,
      opts[:subscribe_after] || Enum.random(3_000..5_000)
    )

    {:producer, state}
  end

  @impl GenStage
  def handle_info(:check_auto_subscribe, state) do
    identifier = "#{inspect(__MODULE__)} (#{inspect(state.self)})"

    if _do_function(state.config.subscribe_on_init?) do
      Logger.info("#{identifier} subscribing to '#{state.config.stream_name}'")

      GenStage.async_info(self(), :subscribe)
    else
      # coveralls-ignore-start
      Logger.info(
        "#{identifier} did not subscribe to '#{state.config.stream_name}'"
      )

      # coveralls-ignore-stop
    end

    {:noreply, [], state}
  end

  def handle_info(:subscribe, state) do
    Kelvin.Listener.subscribe(state.extreme_listener)

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_call({:on_event, event}, from, state) do
    # when the current demand is 0, we should
    case state do
      %{demand: 0, buffer_size: size, max_buffer_size: max}
      when size + 1 == max ->
        {:noreply, [], _enqueue(state, {event, from})}

      %{demand: 0} ->
        {:reply, :ok, [], _enqueue(state, event)}

      %{demand: demand} ->
        {:reply, :ok, [{state.self, event}], put_in(state.demand, demand - 1)}
    end
  end

  @impl GenStage
  def handle_demand(demand, state) do
    _dequeue_events(state, demand, [])
  end

  defp _dequeue_events(%{buffer_size: size} = state, demand, events)
       when size == 0 or demand == 0 do
    {:noreply, :lists.reverse(events), put_in(state.demand, demand)}
  end

  defp _dequeue_events(state, demand, events) do
    case _dequeue(state) do
      {{:value, {event, from}}, state} ->
        GenStage.reply(from, :ok)
        _dequeue_events(state, demand - 1, [{state.self, event} | events])

      {{:value, event}, state} ->
        _dequeue_events(state, demand - 1, [{state.self, event} | events])
    end
  end

  defp _dequeue(state) do
    case :queue.out(state.buffer) do
      # coveralls-ignore-start
      {:empty, buffer} ->
        # coveralls-ignore-stop
        {:empty, %{state | buffer: buffer, buffer_size: 0}}

      {value, buffer} ->
        {value, %{state | buffer: buffer, buffer_size: state.buffer_size - 1}}
    end
  end

  defp _do_function(func) when is_function(func, 0), do: func.()

  defp _do_function({m, f, a}) when is_atom(m) and is_atom(f) and is_list(a) do
    apply(m, f, a)
  end

  defp _enqueue(state, element) do
    %{
      state
      | buffer: :queue.in(element, state.buffer),
        buffer_size: state.buffer_size + 1
    }
  end
end
