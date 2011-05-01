require 'rubygems'
require 'mongo'
require 'connect'

@map = <<-EOS
function() {
  emit(this.Make, {crashes: 1});
}
EOS

@reduce = <<-EOS
function(key, values) {
  var count = 0;
  values.forEach(function(doc) {
    count += doc.crashes;
  })
  return {crashes: count};
}
EOS

# You must have an index on 'Make' if you want to show all
# contained documents. Otherwise, MongoDB complains:
#   "too much data for sort() with no index;
#    add an index or specify a smaller limit"
def find_all(coll, limit = 0)
  puts "Finding all documents sorted by 'Make'"
  opts = {
      :fields => [:_id, :Make],
      :sort => [[:Make, Mongo::DESCENDING]],
      :limit => limit.to_i
  }
  coll.find({}, opts)
end

def find_by_make(coll, make, limit = 0)
  puts "Finding all crashes of '#{make}'"
  opts = {
      :fields => [:_id, :Make],
      :sort => [[:Make, Mongo::DESCENDING]],
      :limit => limit.to_i
  }
  coll.find({:Make => make}, opts)
end

def sum_crashes_with_builtin(coll, make)
  puts "Counting all crashes of '#{make}' (built-in)"
  coll.find({:Make => make})
end

def sum_crashes_with_map_reduce(coll, make)
  puts "Counting all crashes of '#{make}' using Map/Reduce"
  opts = {
      :query => {:Make => make},
      :out => {:inline => true},
      :raw => true
  }
  coll.map_reduce(@map, @reduce, opts)
end

# Runs about 3 times faster than the map/reduce version.
# The result is sorted by the number of crashes.
def group_with_builtin(coll)
  puts "Grouping by 'Make' (built-in)"
  coll.group({
    :key      => [:Make],
    :initial  => {:crashes => 0},
    :reduce   => 'function(doc, prev) {prev.crashes += 1;}'
  })
end

# The result is sorted by Make.
def group_with_map_reduce(coll, limit = 0)
  puts "Grouping by 'Make' (map/reduce)"
  opts = {
      :limit => limit.to_i,
      :out => {:inline => true},
      :raw => true
  }
  coll.map_reduce(@map, @reduce, opts)
end

with_connection('aviation', 'crashes') do |coll|
  find_all(coll, 10)
  #find_by_make(coll, "CESSNA", 10)
  #sum_crashes_with_builtin(coll, 'Boeing')
  #sum_crashes_with_map_reduce(coll, 'Boeing')
  #group_with_builtin(coll)
  #group_with_map_reduce(coll, 10)
end
