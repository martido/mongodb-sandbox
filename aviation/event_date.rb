require 'rubygems'
require 'mongo'
require 'connect'

# Time-based Map/Reduce queries on the complete data set for both the number
# of accidents and the total number of fatalities.
#
# Some things that are different from Cloudant's CouchDB examples:
# (1) The key of the document returned by 'map' cannot be an array.
# (2) Currently, the output of a 'map' function and the value returned by
#     'reduce' cannot be an array.
# (3) A feature like CouchDB's 'group_level' functionaliy must be coded by
#     hand by using different 'map' functions.
# (4) There's no way (afaik) to turn of the reduce in a Map/Reduce.

@map = <<-EOS
function() {
  var eventDate = new Date(Date.parse(this['Event Date']));
  var fatalities = 0;
  if (this['Total Fatal Injuries'] != "") {
    fatalities = parseInt(this['Total Fatal Injuries']);
  }
  emit(@KEY@, {crashes: 1, fatalities: fatalities});
}
EOS

@reduce = <<-EOS
function(key, values) {
  var crashes = 0;
  var fatalities = 0;
  values.forEach(function(doc) {
    crashes += doc.crashes;
    fatalities += doc.fatalities;
  })
  return {crashes: crashes, fatalities: fatalities};
}
EOS

@emit_year_and_month = '{year: eventDate.getFullYear(), month: eventDate.getMonth()}'
@emit_year           = '{year: eventDate.getFullYear()}'
@emit_null           = 'null'

def group_by_year_and_month(coll)
  map = @map.sub('@KEY@', @emit_year_and_month)
  opts = {
      :out => {:inline => true},
      :raw => true
  }
  coll.map_reduce(map, @reduce, opts)
end

def group_by_year_and_month_limit_to_make(coll, make)
  map = @map.sub('@KEY@', @emit_year_and_month)
  opts = {
      :query => {:Make => make},
      :out => {:inline => true},
      :raw => true
  }
  coll.map_reduce(map, @reduce, opts)
end

# db.crashes.find({
#   "Event Date": {
#     $gte : new Date(1994, 0, 1),
#     $lte : new Date(1995, 0, 1)
#   }
# });
def group_by_year_and_month_limit_to_range(coll, from, to)
  map = @map.sub("@KEY@", @emit_year_and_month)
  opts = {
    :query => {
      "Event Date" => {
        '$gte' => from,
        '$lt' => to
      }
    },
    :out => {:inline => true},
    :raw => true
  }
  puts "Options: #{opts.inspect}"
  coll.map_reduce(map, @reduce, opts)
end

def group_by_year(coll)
  map = @map.sub("@KEY@", @emit_year)
  opts = {
    :out => {:inline => true},
    :raw => true
  }
  coll.map_reduce(map, @reduce, opts)
end

def group_by_year_limit_to_make(coll, make)
  map = @map.sub("@KEY@", @emit_year)
  opts = {
      :query => {'Make' => make},
      :out => {:inline => true},
      :raw => true
  }
  coll.map_reduce(map, @reduce, opts)
end

def group_by_null(coll)
  map = @map.sub("@KEY@", @emit_null)
  opts = {
      :out => {:inline => true},
      :raw => true
  }
  coll.map_reduce(map, @reduce, opts)
end

with_connection('aviation', 'crashes') do |coll|
  group_by_year_and_month(coll)
  #group_by_year_and_month_limit_to_make(coll, 'Boeing')
  #group_by_year_and_month_limit_to_range(coll, Time.utc(1994,1), Time.utc(1994,2))
  #group_by_year_and_month_limit_to_range(coll, Time.utc(1994), Time.utc(1995))
  #group_by_year(coll)
  #group_by_year_limit_to_make(coll, 'Boeing')
  #group_by_null(coll)
end
