# frozen_string_literal: true

require 'active_record/connection_adapters/postgresql_adapter'
require 'active_record/pipeline_future_result'
require 'active_record/connection_adapters/postgres_pipeline/pipeline_database_statements'
require 'active_record/connection_adapters/postgres_pipeline/referential_integrity'
require 'active_record/pipeline_errors'

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
        ConnectionAdapters::PostgresPipelineAdapter.new_client(conn_params), logger,
        conn_params, config
      )
    end
  end

  module ConnectionAdapters

    # Establishes a connection to the database of postgres with pipeline support
    class PostgresPipelineAdapter < ActiveRecord::ConnectionAdapters::PostgreSQLAdapter
      ADAPTER_NAME = 'PostgresPipeline'

      include PostgresPipeline::DatabaseStatements
      include PostgresPipeline::ReferentialIntegrity

      def initialize(connection, logger, conn_params, config)
        @piped_results = []
        @counter = 0
        super(connection, logger, conn_params, config)
        connection.enter_pipeline_mode
        @is_pipeline_mode = true
      end

      def is_pipeline_mode?
        @connection.pipeline_status != PG::PQ_PIPELINE_OFF
      end

      def case_insensitive_comparison(attribute, value) # :nodoc:
        column = column_for_attribute(attribute)

        if can_perform_case_insensitive_comparison_for?(column).result
          attribute.lower.eq(attribute.relation.lower(value))
        else
          attribute.eq(value)
        end
      end

      def reconnect!
        initialize_results(nil) if active? && @piped_results.count > 0
        super
        @connection.enter_pipeline_mode
      end

      def disconnect!
        initialize_results(nil) if active? && @piped_results.count > 0
        super
      end

      def reset!
        @lock.synchronize do
          clear_cache!
          reset_transaction
          unless @connection.transaction_status == ::PG::PQTRANS_IDLE
            flush_pipeline_and_get_sync_result { @connection.send_query_params 'ROLLBACK', [] }
            #  @connection.query "ROLLBACK"
          end
          flush_pipeline_and_get_sync_result { @connection.send_query_params 'DISCARD ALL', [] }
          # @connection.query "DISCARD ALL"
          configure_connection
        end
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
            if is_pipeline_mode?
              #If Pipeline mode return future result objects
              @connection.send_query_params(sql, type_casted_binds)
              @connection.pipeline_sync
              future_result = FutureResult.new(self, sql, binds)
              @counter += 1
              @piped_results << future_result
              future_result
            else
              @connection.exec_params(sql, type_casted_binds)
            end
          end
        end
      end

      def pipeline_fetch(future_result)
        @instrumenter.instrument('sql.active_record', sql: "FETCHING PIPELINE RESULT TILL #{future_result.sql}",
                                                      name: 'PIPELINE_FETCH',
                                                      binds: [],
                                                      type_casted_binds: [],
                                                      statement_name: nil,
                                                      connection: self) do
          initialize_results(future_result)
        end
      end

      def prepare_statement(sql, binds)
        @lock.synchronize do
          sql_key = sql_key(sql)

          unless @statements.key? sql_key
            nextkey = @statements.next_key
            begin
              if is_pipeline_mode?
                flush_pipeline_and_get_sync_result { @connection.send_prepare nextkey, sql }
              else
                @connection.prepare nextkey, sql
              end
            rescue => e
              raise translate_exception_class(e, sql, binds)
            end
            # Clear the queue
            unless is_pipeline_mode?
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
            if is_pipeline_mode?
              @connection.send_query_prepared(stmt_key, type_casted_binds)
              @connection.pipeline_sync
              future_result = FutureResult.new(self, sql, binds)
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

      # def active?
      #   # Need to implement
      #   true
      # end

      def active?
        @lock.synchronize do
          flush_pipeline_and_get_sync_result { @connection.send_query_params 'SELECT 1' , [] }
        end
        true
      rescue PG::Error
        false
      end

      def request_in_error(result_status)
        [PG::PGRES_FATAL_ERROR].include? result_status
      end

      def request_in_aborted(result_status)
        [PG::PGRES_PIPELINE_ABORTED].include? result_status
      end

      def transaction_in_error?(transaction_status)
        [PG::PQTRANS_INERROR].include? transaction_status
      end

      ENDLESS_LOOP_SECONDS = 20
      def initialize_results(required_future_result)
        @lock.synchronize do
          @connection.pipeline_sync
          time_since_last_result = Time.now
          future_result = nil
          begin
            loop do
              result = @connection.get_result
              if response_received(result)
                time_since_last_result = Time.now
                future_result = @piped_results.shift
                future_result.assign(result)
                break if required_future_result == future_result && !@piped_results.empty?
              elsif pipeline_in_sync?(result) && @piped_results.empty?
                break
              elsif transaction_in_error?(@connection.transaction_status)
                @logger.error "Transaction status in error #{@connection.transaction_status}, expecting the status to cleaned up in next pipeline invocation"
                break
              elsif request_in_error(result.try(:result_status))
                future_result = @piped_results.shift
                @logger.error "Raising error because future for query #{future_result.sql} called at stack : #{future_result.execution_stack} gave result #{result.try(:result_status)}"
                raise PipelineError.new(result.error_message, result)
              elsif request_in_aborted(result.try(:result_status))
                future_result = @piped_results.shift
                future_result.assign_error(PriorQueryPipelineError.new('A previous query has made the pipeline in aborted state', result))
                @logger.info "Setting PriorQueryPipelineError for sql #{future_result.sql} called at stack : #{future_result.execution_stack}"
                break if required_future_result == future_result
              elsif (Time.now - time_since_last_result).to_i > ENDLESS_LOOP_SECONDS
                @logger.debug "Seems like an endless loop with Pipeline Sync status #{pipeline_in_sync?(result)}, piped results size : #{@piped_results.count}, connection pipeline : #{@connection.inspect} , result :#{result.inspect}"
              end
            end
          rescue ActiveRecord::PipelineError => e
            handle_pipeline_error(e, future_result)
          end
        end
      end

      def is_cached_plan_failure?(pgerror)
        pgerror.result.result_error_field(PG::PG_DIAG_SQLSTATE) == FEATURE_NOT_SUPPORTED &&
          pgerror.result.result_error_field(PG::PG_DIAG_SOURCE_FUNCTION) == 'RevalidateCachedQuery'
      rescue
        false
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
        if is_pipeline_mode?
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

      def exec_query(sql, name = 'SQL', binds = [], prepare: false)
        execute_and_clear(sql, name, binds, prepare: prepare) do |result|
          if !result.is_a?(FutureResult)
            build_ar_result(result)
          else
            result
          end
        end
      end

      def build_statement_pool
        StatementPool.new(@connection, self.class.type_cast_config_to_integer(@config[:statement_limit]), self)
      end

      class StatementPool < ConnectionAdapters::PostgreSQLAdapter::StatementPool # :nodoc:
        def initialize(connection, max, adapter)
          super(connection, max)
          @connection = connection
          @counter = 0
          @adapter = adapter
        end

        private
        def dealloc(key)
          @adapter.flush_pipeline_and_get_sync_result { @connection.send_query_params "DEALLOCATE #{key}", [] } if connection_active?
          # @connection.query "DEALLOCATE #{key}"
        rescue PG::Error
        end
      end

      def flush_pipeline_and_get_sync_result
        @lock.synchronize do
          initialize_results(nil)
          yield
          @connection.pipeline_sync
          get_pipelined_result
        end
      end

      private

      MULTIPLE_QUERY = '42601'
      def translate_exception(exception, message:, sql:, binds:)
        return exception unless exception.respond_to?(:result)

        case exception.result.try(:error_field, PG::PG_DIAG_SQLSTATE)
        when MULTIPLE_QUERY
          MultipleQueryError.new(message)
        else
          super
        end
      end

      def pipeline_in_sync?(result)
        result.try(:result_status) == PG::PGRES_PIPELINE_SYNC
      end

      def response_received(result)
        [PG::PGRES_TUPLES_OK, PG::PGRES_COMMAND_OK].include?(result.try(:result_status))
      end

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

      def handle_pipeline_error(exception, future_result)
        activerecord_error = translate_exception_class(exception, future_result.sql, future_result.binds)
        future_result.assign_error(activerecord_error)
        raise activerecord_error unless is_cached_plan_failure?(exception)

        # Nothing we can do if we are in a transaction because all commands
        # will raise InFailedSQLTransaction
        if in_transaction?
          raise ActiveRecord::PreparedStatementCacheExpired.new(exception.message)
        else
          @lock.synchronize do
            # outside of transactions we can simply flush this query and retry
            @statements.delete sql_key(future_result.sql)
          end
        end
        raise activerecord_error
      end

      def get_pipelined_result
        result = nil
        time_since_last_result = Time.now

        loop do
          interim_result = @connection.get_result
          if response_received(interim_result)
            result = interim_result
          elsif transaction_in_error?(@connection.transaction_status)
            break
          elsif request_in_error(interim_result.try(:result_status))
            raise PipelineError.new(interim_result.error_message, interim_result)
          elsif request_in_aborted(interim_result.try(:result_status))
            @logger.warn 'Not expecting pipeline to go in aborted state, as everything is flushed'
          elsif ((Time.now - time_since_last_result) % ENDLESS_LOOP_SECONDS).zero?
            @logger.debug "Seems like an endless loop with Pipeline Sync status #{pipeline_in_sync?(result)}, connection pipeline : #{@connection.inspect} , result :#{interim_result.inspect}"
          end
          break if pipeline_in_sync?(interim_result) && result
        end
        result
      end

      ActiveRecord::Type.add_modifier({ array: true }, OID::Array, adapter: :postgrespipeline)
      ActiveRecord::Type.add_modifier({ range: true }, OID::Range, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:bit, OID::Bit, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:bit_varying, OID::BitVarying, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:binary, OID::Bytea, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:cidr, OID::Cidr, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:date, OID::Date, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:datetime, OID::DateTime, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:decimal, OID::Decimal, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:enum, OID::Enum, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:hstore, OID::Hstore, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:inet, OID::Inet, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:interval, OID::Interval, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:jsonb, OID::Jsonb, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:money, OID::Money, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:point, OID::Point, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:legacy_point, OID::LegacyPoint, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:uuid, OID::Uuid, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:vector, OID::Vector, adapter: :postgrespipeline)
      ActiveRecord::Type.register(:xml, OID::Xml, adapter: :postgrespipeline)
    end
  end
end