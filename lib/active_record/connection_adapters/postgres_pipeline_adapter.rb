# frozen_string_literal: true
require 'active_record/connection_adapters/postgresql_adapter'

module ActiveRecord
  module ConnectionHandling # :nodoc:
    # Establishes a connection to the database that's used by all Active Record objects
    def pipeline_postgresql_connection(config)
      conn_params = config.symbolize_keys.compact

      # Map ActiveRecords param names to PGs.
      conn_params[:user] = conn_params.delete(:username) if conn_params[:username]
      conn_params[:dbname] = conn_params.delete(:database) if conn_params[:database]

      # Forward only valid config params to PG::Connection.connect.
      valid_conn_param_keys = PG::Connection.conndefaults_hash.keys + [:requiressl]
      conn_params.slice!(*valid_conn_param_keys)

      ConnectionAdapters::PostgresPipelineAdapter.new(
        ConnectionAdapters::PostgresPipelineAdapter.new_client(conn_params),
        logger,
        conn_params,
        config,
        )
    end
  end

  module ConnectionAdapters

    # Establishes a connection to the database of postgres with pipeline support
    class PostgresPipelineAdapter < ActiveRecord::ConnectionAdapters::PostgreSQLAdapter
      ADAPTER_NAME = 'PostgresSQLWithPipeline'

      def initialize(connection, logger, connection_parameters, config)
        super(connection, logger, connection_parameters, config)
        connection.enter_pipeline_mode
      end

      class << self
        def new_client(conn_params)
          pg_connect = PostgreSQLAdapter.new_client(conn_params)
          pg_connect
        end
      end

    end
  end
end

