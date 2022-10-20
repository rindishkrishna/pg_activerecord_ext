require 'spec_helper'
require 'active_record'
require 'active_record/connection_adapters/postgres_pipeline_adapter'
require 'active_record/pipeline_relation'
require 'model/author'
require 'model/user'

RSpec.describe 'ActiveRecord::Relation' do
  before(:all) do
    @connection = ActiveRecord::Base.postgresql_connection(min_messages: 'warning')
    @connection.drop_table(:users, if_exists: true)
    @connection.drop_table(:authors, if_exists: true)
    @connection.create_table(:users, id: :string, limit: 42, force: true)
    @connection.create_table :authors do |t|
      t.column :user_id, :string
    end
    ActiveRecord::Base.establish_connection("adapter" => "postgresql")
    @user = User.create(id: 3)
    @author = Author.create(id: 4, user_id: "3")
  end


  it 'should fetch results for where clause in pipeline mode when load_in_pipeline is called' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    result =   User.where("id is not null").load_in_pipeline
    expect(result.first.id).to eq("3")
  end

  it 'should fetch results for where clause in pipeline mode even when load_in_pipeline is not explicity called' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    result =   User.where("id is not null")
    expect(result.first.id).to eq("3")
  end

  it 'should fetch results  when all queries are loaded in pipeline' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    result_1 =  User.where("id is not null").load_in_pipeline
    result_2 =  User.where("id = '3'").load_in_pipeline
    expect(result_1.first.id).to eq("3")
    expect(result_2.first.id).to eq("3")
  end

  it 'should fetch results when some queries are loaded in pipeline' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    result_1 =  User.where("id is not null")
    result_2 =  User.where("id = '3'").load_in_pipeline
    expect(result_1.first.id).to eq("3")
    expect(result_2.first.id).to eq("3")
  end
  it 'should fetch results for dependent queries' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    users =   User.where("id is not null").load_in_pipeline
    authors = Author.where(user_id: users.first.id)
    expect(authors.first).to eq( @author)
  end

end