require 'rubygems'
require 'mongo'
require 'connect'

def find_by_id(coll, id)
  puts "Finding crash with ID #{id}"
  coll.find_one(:_id => BSON::ObjectId.from_string(id))
end

def find_by_range(coll, from, to)
  puts "Finding crashes between #{from} and #{to}"
  opts = {
    :query => {
      :"Event Date" => {
        '$gte' => from,
        '$lt' => to
      }
    }
  }
  puts "Options: #{opts.inspect}"
  coll.find(opts)
end

with_connection('aviation', 'crashes') do |coll|
  find_by_range(coll, Time.utc(1994,1,1), Time.utc(1994,1,2))
end
