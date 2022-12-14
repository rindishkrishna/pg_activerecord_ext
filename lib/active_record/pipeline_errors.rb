# frozen_string_literal: true
require "active_record/errors"

module ActiveRecord
  class PipelineError < StandardError
    attr_reader :result

    def initialize(message, result = nil)
      super(message)
      @result = result
    end
  end

  class MultipleQueryError < StatementInvalid
  end
end
