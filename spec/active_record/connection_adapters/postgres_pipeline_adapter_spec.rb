# frozen_string_literal: true
require 'spec_helper'
require 'active_record'
require 'active_record/connection_adapters/postgres_pipeline_adapter'

RSpec.describe 'ActiveRecord::ConnectionAdapters::PostgresPipelineAdapter' do
  it 'pipeline mode should be on in connection' do
    connection = ActiveRecord::Base.pipeline_postgresql_connection(min_messages: 'warning')
    raw_conn = connection.instance_variable_get(:@connection)

    expect(raw_conn.pipeline_status).to eq PG::PQ_PIPELINE_ON
  end

  it 'should send the query in pipeline mode' do
    connection = ActiveRecord::Base.pipeline_postgresql_connection(min_messages: 'warning')
    connection.exec_query("SELECT 1")

    piped_results = connection.instance_variable_get(:@piped_results)
    expect(piped_results.try(:length)).to eq 1
  end

  it 'should return error if one of the query is incorrect' do

  end

  it 'should assign the results back in the same order, as it was called' do

  end
end
