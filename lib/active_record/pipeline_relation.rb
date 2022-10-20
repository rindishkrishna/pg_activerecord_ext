# frozen_string_literal: true

module ActiveRecord
  # = Active Record \Relation
  class Relation

    def load_in_pipeline
      return load if !connection.pipeline_enabled?
      unless loaded?

        result = exec_main_query

        if result.is_a?(Array)
          @records = result
        else
          @future_result = result
        end
        @loaded = true
      end
      self
    end

    def exec_queries(&block)

      skip_query_cache_if_necessary do
        rows = if scheduled?
                 future = @future_result
                 @future_result = nil
                 future.result
               else
                 exec_main_query
               end
        # This is to handle a case where load_in_pipeline is not called before trying to read the result
        if rows.is_a?(ActiveRecord::FutureResult)
          rows = rows.result
        end

        records = instantiate_records(rows, &block)
        preload_associations(records) unless skip_preloading_value

        records.each(&:readonly!) if readonly_value
        records.each { |record| record.strict_loading!(strict_loading_value) } unless strict_loading_value.nil?

        records
      end
    end
  end
end