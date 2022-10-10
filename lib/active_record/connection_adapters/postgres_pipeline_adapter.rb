# frozen_string_literal: true

require 'active_record/connection_adapters/postgresql_adapter'

module ActiveRecord
  module ConnectionHandling # :nodoc:
    # Establishes a connection to the database that's used by all Active Record objects
    def pipeline_postgresql_connection(config)
      conn_params = config.symbolize_keys.compact
      conn_params[:user] = conn_params.delete(:username) if conn_params[:username]
      conn_params[:dbname] = conn_params.delete(:database) if conn_params[:database]
      valid_conn_param_keys = PG::Connection.conndefaults_hash.keys + [:requiressl]
      conn_params.slice!(*valid_conn_param_keys)

      ConnectionAdapters::PostgresPipelineAdapter.new(
        ConnectionAdapters::PostgreSQLAdapter.new_client(conn_params), logger,
        conn_params, config
      )
    end
  end

  module ConnectionAdapters

    # Establishes a connection to the database of postgres with pipeline support
    class PostgresPipelineAdapter < ActiveRecord::ConnectionAdapters::PostgreSQLAdapter
      ADAPTER_NAME = 'PostgresSQLWithPipeline'

      def initialize(connection, logger, conn_params, config)
        super(connection, logger, conn_params, config)
        connection.enter_pipeline_mode
        @is_pipeline_mode = true
        @piped_results = Queue.new
        @counter = 0
      end

      def exec_no_cache(sql, name, binds, async: false)
        materialize_transactions
        mark_transaction_written_if_write(sql)

        # make sure we carry over any changes to ActiveRecord.default_timezone that have been
        # made since we established the connection
        update_typemap_for_default_timezone

        type_casted_binds = type_casted_binds(binds)
        log(sql, name, binds, type_casted_binds, async: async) do
          ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
            if @connection.pipeline_status == PG::PQ_PIPELINE_ON
              @connection.send_query_params(sql, type_casted_binds)

              future_result = FutureResult.new
              @counter += 1
              @piped_results << { order: @counter, result: future_result }
              future_result
            else
              @connection.exec_params(sql, type_casted_binds)
            end
          end
        end
      end

      def initialize_results
        #Option 1 : A separate thread which keeps on checking results and initialize FutureResult objects
        # while(true) do
        #   if(@piped_results.length() > 1)
        #      result = @connection.get_result()
        #      if(!result.empty())
        #        future_result << @piped_results.pop.result
        #        future_result.assign(build_ar_result(result))
        #      end
        #   end
        # end


        #Option 2
        # while(TIMEOUT) do
        #         #   if(@piped_results.length() > 1)
        #         #      result = @connection.get_result()
        #         #      if(!result.empty())
        #         #        future_result << @piped_results.pop.result
        #         #        future_result.assign(result)
        #         #      end
        #         #   end
        #         # end
      end

      def exec_query(sql, name = "SQL", binds = [], prepare: false, async: false) # :nodoc:
        execute_and_clear(sql, name, binds, prepare: prepare, async: async) do |result|
          if !result.is_a?(FutureResult)
            build_ar_result(result)
          else
            result
          end
        end
      end

      private

      def build_ar_result(result)
        types = {}
        fields = result.fields
        fields.each_with_index do |fname, i|
          ftype = result.ftype i
          fmod = result.fmod i
          case type = get_oid_type(ftype, fmod, fname)
          when Type::Integer, Type::Float, OID::Decimal, Type::String, Type::DateTime, Type::Boolean
            # skip if a column has already been type casted by pg decoders
          else
            types[fname] = type
          end
        end
        build_result(columns: fields, rows: result.values, column_types: types)
      end
    end
  end
end
