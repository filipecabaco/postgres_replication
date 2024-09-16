defmodule InspectCalls.MixProject do
  use Mix.Project

  def project do
    [
      app: :inspect_calls,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {InspectCalls, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:postgres_replication, path: "../../"}
    ]
  end
end