# frozen_string_literal: true
require 'active_record/connection_adapters/postgresql_adapter'

module ActiveRecord
  module ConnectionHandling # :nodoc:
    # Establishes a connection to the database that's used by all Active Record objects
    def pipeline_postgresql_connection(config)
      ConnectionAdapters::PostgresPipelineAdapter.new(nil, nil, nil, config)
    end
  end

  module ConnectionAdapters

    # Establishes a connection to the database of postgres with pipeline support
    class PostgresPipelineAdapter < ActiveRecord::ConnectionAdapters::PostgreSQLAdapter
      ADAPTER_NAME = 'PostgresSQLWithPipeline'

      def initialize(connection, logger, connection_parameters, config)
        super(connection, logger, connection_parameters, config)
      end

      class << self
        def new_client(conn_params)
          pg_connect = PostgreSQLAdapter.new_client(conn_params)
          pg_connect.enter_pipeline_mode
        end
      end
    end
  end
end

