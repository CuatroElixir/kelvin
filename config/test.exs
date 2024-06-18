import Config

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  level: :debug,
  metadata: [:pid, :module, :function]

config :ex_unit, capture_log: true

config :kelvin, ExtremeClient,
  db_type: :node,
  host: System.get_env("EVENTSTORE_HOST") || "localhost",
  port: 1113,
  username: "admin",
  password: "changeit",
  reconnect_delay: 2_000,
  max_attempts: :infinity
