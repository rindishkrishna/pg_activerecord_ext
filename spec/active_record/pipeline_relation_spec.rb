require 'spec_helper'
require 'pg_activerecord_ext'
require 'model/author'
require 'model/user'

GREEN   = "\e[32m"
BLUE    = "\e[34m"
CLEAR   = "\e[0m"
color = GREEN

RSpec.describe 'ActiveRecord::Relation' do
  before(:each) do
    color = (color == GREEN) ? BLUE : GREEN
  end
  before(:all) do
    @connection = ActiveRecord::Base.postgresql_connection(min_messages: 'warning')
    @connection.drop_table(:users, if_exists: true)
    @connection.drop_table(:authors, if_exists: true)
    @connection.create_table(:users, id: :string, limit: 42, force: true) do |t|
      t.column :description, :string
    end
    @connection.create_table :authors do |t|
      t.column :user_id, :string
    end
    ActiveRecord::Base.establish_connection("adapter" => "postgresql")
    @user_1 = User.create(id: 3)
    @user_2 = User.create(id: 4)
    @author = Author.create(id: 4, user_id: "3")
    @callback = lambda {|*args| Logger.new(STDOUT).debug("#{color} #{args.last[:sql]} #{CLEAR}" )  unless args.last[:name] == "SCHEMA" }
  end


  it 'should fetch results for where clause in pipeline mode when load_in_pipeline is called' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    ActiveSupport::Notifications.subscribed( @callback, "sql.active_record") do
      users = User.where("id is not null").load_in_pipeline
      expect(users).to eq([@user_1, @user_2])
    end
  end

  it 'should fetch results for where clause in pipeline mode even when load_in_pipeline is not explicity called' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    ActiveSupport::Notifications.subscribed( @callback, "sql.active_record") do
      users = User.where("id is not null")
      expect(users).to eq([@user_1, @user_2])
    end

  end

  it 'should fetch results  when all queries are loaded in pipeline' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    ActiveSupport::Notifications.subscribed(@callback, "sql.active_record") do
      users_1 =  User.where("id is not null").load_in_pipeline
      users_2 =  User.where("id = '4'").load_in_pipeline
      expect(users_1).to eq([@user_1, @user_2])
      expect(users_2.first).to eq(@user_2)
    end
  end

  it 'should fetch results when some queries are loaded in pipeline' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    @sql = []
    track_sql_queries = lambda do |*args|
      unless args.last[:name] == "SCHEMA"
        Logger.new(STDOUT).debug("#{color} #{args.last[:sql]} #{CLEAR}" )
        @sql << args.last[:sql]
      end
    end
    ActiveSupport::Notifications.subscribed( track_sql_queries, "sql.active_record") do
      users_1 =  User.where("id is not null")
      users_2 =  User.where("id = '4'").load_in_pipeline
      expect(users_1).to eq([@user_1, @user_2])
      expect(users_2.first).to eq(@user_2)
    end
    expect(@sql.first).to eq("SELECT \"users\".* FROM \"users\" WHERE (id = '4')")
    expect(@sql.last).to eq("SELECT \"users\".* FROM \"users\" WHERE (id is not null)")
  end

  it 'should fetch results for dependent queries' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    ActiveSupport::Notifications.subscribed( @callback, "sql.active_record") do
      users =   User.where("id is not null").load_in_pipeline
      authors = Author.where(user_id: users.first.id)
      expect(authors.first).to eq( @author)
    end
  end

end