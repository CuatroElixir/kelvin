defmodule Kelvin.Listener do
  @moduledoc false
  use Extreme.ListenerWithBackPressure

  @impl Extreme.ListenerWithBackPressure
  def on_init(opts) do
    state = %{
      producer: Keyword.fetch!(opts, :producer),
      get_stream_position_fun: Keyword.fetch!(opts, :get_stream_position_fun)
    }

    {:ok, state}
  end

  @impl Extreme.ListenerWithBackPressure
  def get_last_event(_stream_name, %{} = state),
    do: state.get_stream_position_fun.()

  @impl Extreme.ListenerWithBackPressure
  def process_push(push, _stream_name, %{} = state),
    do: GenServer.call(state.producer, {:on_event, push}, :infinity)
end
