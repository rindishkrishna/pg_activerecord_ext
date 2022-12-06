module ActiveRecord
  module ConnectionAdapters
    module PostgresPipeline
      module DatabaseStatements
        # Executes an SQL statement, returning a PG::Result object on success
        # or raising a PG::Error exception otherwise.
        # Note: the PG::Result object is manually memory managed; if you don't
        # need it specifically, you may want consider the <tt>exec_query</tt> wrapper.
        def execute(sql, name = nil)
          if preventing_writes? && write_query?(sql)
            raise ActiveRecord::ReadOnlyError, "Write query attempted while in readonly mode: #{sql}"
          end

          materialize_transactions
          mark_transaction_written_if_write(sql)

          log(sql, name) do
            ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
              if @connection.pipeline_status == PG::PQ_PIPELINE_ON
                # Refactor needed
                initialize_results(nil)
                #                sql = sql.strip.squish
                sql.split(';').each do |query|
                  @connection.send_query_params(query, [])
                end
                @connection.pipeline_sync
                result = nil
                sql.split(';').each do
                  result = @connection.get_result
                  @connection.get_result
                end
                # When the results of all the queries in the pipeline have been returned, get_result returns a result containing the status value PGRES_PIPELINE_SYNC
                @connection.get_result
                result
              else
                @connection.async_exec(sql)
              end
            end
          end
        end

        def query(sql, name = nil) #:nodoc:
          materialize_transactions
          mark_transaction_written_if_write(sql)

          log(sql, name) do
            ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
              if @connection.pipeline_status == PG::PQ_PIPELINE_ON
                initialize_results(nil)
                @connection.send_query_params(sql, [])
                #Refactor needed
                @connection.send_flush_request
                result = @connection.get_result
                @connection.get_result
                result.map_types!(@type_map_for_results).values
              else
                @connection.async_exec(sql).map_types!(@type_map_for_results).values
              end
            end
          end
        end

        def exec_delete(sql, name = nil, binds = [])
          execute_and_clear(sql, name, binds) do |result|
            result.cmd_tuples
          end
        end
        alias :exec_update :exec_delete


      end
    end
  end
end