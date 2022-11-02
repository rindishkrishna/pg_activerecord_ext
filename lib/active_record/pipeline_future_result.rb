# frozen_string_literal: true

module ActiveRecord
  class FutureResult # :nodoc:
    def initialize(connection_adapter)
      @connection_adapter = connection_adapter
      @result = nil
      @event_buffer = nil
      @error = nil
      @pending = true
    end

    def result
      # Wait till timeout until pending is false
      return @result unless @pending

      @connection_adapter.initialize_results(self)
      @pending = false
      @result
    end

    def assign(result)
      @pending = false
      @result =  result
    end

    def clear; end
  end
end
