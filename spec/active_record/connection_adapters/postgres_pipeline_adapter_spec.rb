# frozen_string_literal: true
require 'spec_helper'
require 'active_record'
require 'active_record/connection_adapters/postgres_pipeline_adapter'

RSpec.describe 'ActiveRecord::ConnectionAdapters::PostgresPipelineAdapter' do
  it 'pipeline mode should be on' do
    connection = ActiveRecord::ConnectionHandling.postgresql_connection(host: File::NULL)
    raw_conn = connection.instance_variable_get(:@raw_connection)

    assert_equal PQ_PIPELINE_ON, raw_conn.pipeline_status
  end
end
