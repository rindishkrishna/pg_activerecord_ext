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
    connection.exec_query('SELECT 1')

    piped_results = connection.instance_variable_get(:@piped_results)
    expect(piped_results.try(:length)).to eq 1
  end

  describe 'Using exec_query' do
    it 'should assign the results back in the same order, as it was called' do
      # Given
      connection = ActiveRecord::Base.postgresql_connection(min_messages: 'warning')
      connection.exec_query('CREATE TABLE postgresql_partitioned_table_parent (
      id SERIAL PRIMARY KEY,
      number integer
      );')
      connection.exec_query('CREATE TABLE postgresql_partitioned_table ( )INHERITS (postgresql_partitioned_table_parent);')

      result = connection.exec_insert('insert into postgresql_partitioned_table_parent (number) VALUES (1)', nil, [], 'id', 'postgresql_partitioned_table_parent_id_seq')

      # Invocation
      pipeline_connection = ActiveRecord::Base.pipeline_postgresql_connection(min_messages: 'warning')
      expect = pipeline_connection.exec_query('select max(id) from postgresql_partitioned_table_parent')

      # Assertions
      assert_equal expect.result, result.rows.first.first
    end

    it 'should return error if one of the query is incorrect' do

    end
  end

  describe 'Using query' do
    it 'should assign the results back in the same order, as it was called' do
      assert_equal 1, 2
    end
  end
end
