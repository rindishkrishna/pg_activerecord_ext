# pg_activerecord_ext
Extension to Postgres ActiveRecord adapter to add new features like pipelining. 

### Assumptions
* The intention is to use it for proof of concept, so advanced and edge scenarios are not covered.
* The activerecord version (7.0.4) has connections which are shared by invocations in same thread. 
  This becomes the basis of the approach as pipelining feature in ruby-pg uses async mechanism and keeps on checking the response
* The performance gain, as observed in tests is ``(n-1) * network_latency`` where n is no of INDEPENDENT queries
* Thorough testing has not been done.

### Community Help
The extension was discussed on rails forum which can be seen here
[proposal for postgres pipeline in active record](https://discuss.rubyonrails.org/t/proposal-adding-postgres-pipeline-support-in-activerecord/81427/2)
some of the suggestions are being taken into consideration, while designing the extension

### Setup
* Install Ruby version > 3.0.0 using version managers
* Install postgres `brew install postgresql`
* Run `bundle install`
* Start postgres locally `brew services start postgresql`
* Create database with your username. `psql postgres` & then `create database <username>`
* Run tests `bundle exec rspec`


