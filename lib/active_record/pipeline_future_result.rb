# frozen_string_literal: true
module ActiveRecord
  class FutureResult # :nodoc:
    attr_accessor :block

    KLASSES = [ ActiveRecord::Result, Array , Integer]

    wrapping_methods = (KLASSES.inject([]) { |result, klass| result + klass.instance_methods(false) } + [:dup] - [:==]).uniq

    wrapping_methods.each do |method|
      define_method(method) do |*args, &block|
        result if @pending
        @result.send(method, *args, &block)
      end
    end

    def initialize(connection_adapter)
      @connection_adapter = connection_adapter
      @result = nil
      @event_buffer = nil
      @error = nil
      @pending = true
      @block = nil
    end

    def result
      # Wait till timeout until pending is false
      return @result unless @pending

      @connection_adapter.initialize_results(self)
      @result
    end

    def assign(result)
      @result = result
      @result = @block.call(result) if @block
      @pending = false
    end

    def ==(other)
      if other.is_a?(ActiveRecord::FutureResult)
        super
      else
        result if @pending
        @result == other
      end
    end
    def clear; end

    def to_s
      result if @pending
      super
    end

    # def ==(other)
    #   result if @pending
    #   @result == other
    # end


    private
    def respond_to_missing?(name, include_private = false)
      result unless @result
      @result.respond_to?(name, include_private)
    end

    def method_missing(method, *args, &block)
      result if @pending
      @result.send(method, *args, &block)
    end
  end
end
