# frozen_string_literal: true

module ConnectionAdapters
  module ConnectionHandling # :nodoc:
    # Establishes a connection to the database that's used by all Active Record objects
    def pipeline_postgresql_connection(config)
      ConnectionAdapters::PostgresPipelineAdapter.new(config)
    end
  end

  # Establishes a connection to the database of postgres with pipeline support
  class PostgresPipelineAdapter < PostgreSQLAdapter
    ADAPTER_NAME = 'PostgresSQLWithPipeline'

    def initialize(config)
      super()
    end
  end
end
