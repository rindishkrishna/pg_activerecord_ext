# frozen_string_literal: true

require 'active_record/connection_adapters/postgresql_adapter'
require 'active_record/pipeline_future_result'
require "active_record/connection_adapters/postgres_pipeline/pipeline_database_statements"

module ActiveRecord
  module ConnectionHandling # :nodoc:
    # Establishes a connection to the database that's used by all Active Record objects
    def postgres_pipeline_connection(config)
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
      ADAPTER_NAME = 'PostgresPipeline'

      include PostgresPipeline::DatabaseStatements

      def initialize(connection, logger, conn_params, config)
        super(connection, logger, conn_params, config)
        connection.enter_pipeline_mode
        @is_pipeline_mode = true
        @piped_results = []
        @counter = 0
      end

      def exec_no_cache(sql, name, binds)
        materialize_transactions
        mark_transaction_written_if_write(sql)

        # make sure we carry over any changes to ActiveRecord::Base.default_timezone that have been
        # made since we established the connection
        update_typemap_for_default_timezone

        type_casted_binds = type_casted_binds(binds)
        log(sql, name, binds, type_casted_binds) do
          ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
            if @connection.pipeline_status == PG::PQ_PIPELINE_ON
              #If Pipeline mode return future result objects
              @connection.send_query_params(sql, type_casted_binds)
              future_result = FutureResult.new(self)
              @counter += 1
              @piped_results << future_result
              future_result
            else
              @connection.exec_params(sql, type_casted_binds)
            end
          end
        end
      end

      def prepare_statement(sql, binds)
        @lock.synchronize do
          sql_key = sql_key(sql)

          unless @statements.key? sql_key
            nextkey = @statements.next_key
            begin
              if @connection.pipeline_status == PG::PQ_PIPELINE_ON
                @connection.send_prepare nextkey, sql
                #Refactor needed
                @connection.send_flush_request
                @connection.get_result.result_status
                @connection.get_result
              else
                @connection.prepare nextkey, sql
              end
            rescue => e
              raise translate_exception_class(e, sql, binds)
            end
            # Clear the queue
            unless @connection.pipeline_status == PG::PQ_PIPELINE_ON
              @connection.get_last_result
            end

            @statements[sql_key] = nextkey
          end
          @statements[sql_key]
        end
      end

      def exec_cache(sql, name, binds)
        materialize_transactions
        mark_transaction_written_if_write(sql)
        update_typemap_for_default_timezone
        stmt_key = prepare_statement(sql, binds)
        type_casted_binds = type_casted_binds(binds)

        log(sql, name, binds, type_casted_binds, stmt_key) do
          ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
            if @connection.pipeline_status == PG::PQ_PIPELINE_ON
              @connection.send_query_params(sql, type_casted_binds)
              future_result = FutureResult.new(self)
              @counter += 1
              @piped_results << future_result
              future_result
            else
              @connection.exec_prepared(stmt_key, type_casted_binds)
            end
          end
        end
      rescue ActiveRecord::StatementInvalid => e
        raise unless is_cached_plan_failure?(e)

        # Nothing we can do if we are in a transaction because all commands
        # will raise InFailedSQLTransaction
        if in_transaction?
          raise ActiveRecord::PreparedStatementCacheExpired.new(e.cause.message)
        else
          @lock.synchronize do
            # outside of transactions we can simply flush this query and retry
            @statements.delete sql_key(sql)
          end
          retry
        end
      end

      def active?
        # Need to implement
        true
      end

      def pipeline_enabled?
        @connection.pipeline_status == PG::PQ_PIPELINE_ON
      end

      def initialize_results(required_future_result)
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
        @connection.pipeline_sync
        loop do
          result =  @connection.get_result
          if [PG::PGRES_TUPLES_OK , PG::PGRES_PIPELINE_ABORTED , PG::PGRES_COMMAND_OK , PG::PGRES_FATAL_ERROR].include?(result.try(:result_status))
            future_result = @piped_results.shift
            #  result = (future_result.format == "ar_result") ? build_ar_result(result) : result
            future_result.assign(result)
            break if required_future_result == future_result && !@piped_results.empty?
          elsif result.try(:result_status) == PG::PGRES_PIPELINE_SYNC && @piped_results.empty?
            break
          end
        end
      end

      def execute_and_clear(sql, name, binds, prepare: false, process_later: false , &block)
        if preventing_writes? && write_query?(sql)
          raise ActiveRecord::ReadOnlyError, "Write query attempted while in readonly mode: #{sql}"
        end

        if !prepare || without_prepared_statement?(binds)
          result = exec_no_cache(sql, name, binds)
        else
          result = exec_cache(sql, name, binds)
        end
        # if @connection.pipeline_status == PG::PQ_PIPELINE_ON
        #   result
        # else
        if @connection.pipeline_status == PG::PQ_PIPELINE_ON
          result.block = block
          return result
        else
          begin
            ret = yield result
          ensure
            result.clear
          end
          ret
        end
        ret
      end

      def exec_query(sql, name = "SQL", binds = [], prepare: false)
        execute_and_clear(sql, name, binds, prepare: prepare) do |result|
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
          else types[fname] = type
          end
        end
        build_result(columns: fields, rows: result.values, column_types: types)
      end
    end
  end
end