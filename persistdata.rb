require 'elasticsearch'
require 'typhoeus'
require 'typhoeus/adapters/faraday'
require 'singleton'

class Persistdata
 include Singleton

 def initialize
   transport_configuration = lambda do |f|
	 f.response :logger
	 f.adapter  :typhoeus
   end
   transport = Elasticsearch::Transport::Transport::HTTP::Faraday.new \
	 hosts: [ { host: 'localhost', port: '9200' } ],
	 &transport_configuration
	@client = Elasticsearch::Client.new transport: transport
 end

 def insert_data(obj)
 	@client.create index: 'json_store',
             type: 'insert_json',
             body: obj
    rescue => e
    	p "Data persistence failed while inserting data."
 end

end