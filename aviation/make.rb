module Aviation
  class Make < Aviation::Crashes

    def initialize(database_name, collection_name)
      super(database_name, collection_name)

      @map = <<-EOS.undent
        function() {
          emit(this.Make, {crashes: 1});
        }
      EOS

      @reduce = <<-EOS.undent
        function(key, values) {
          var count = 0;
          values.forEach(function(doc) {
            count += doc.crashes;
          })
          return {crashes: count};
        }
      EOS
    end

    # You must have an index on 'Make' if you want to show all
    # contained documents. Otherwise, MongoDB complains:
    #   "too much data for sort() with no index;
    #    add an index or specify a smaller limit"
    def find_all(limit = 0)
      puts "Finding all documents sorted by 'Make'"
      opts = {
          :fields => [:_id, :Make],
          :sort => [[:Make, Mongo::DESCENDING]],
          :limit => limit.to_i
      }
      with_connection { |coll| coll.find({}, opts) }
    end

    def find_by_make(make, limit = 0)
      puts "Finding all crashes of '#{make}'"
      opts = {
          :fields => [:_id, :Make],
          :sort => [[:Make, Mongo::DESCENDING]],
          :limit => limit.to_i
      }
      with_connection { |coll| coll.find({:Make => make}, opts) }
    end

    def sum_crashes_with_builtin(make)
      puts "Counting all crashes of '#{make}' (built-in)"
      with_connection { |coll| coll.find({:Make => make}) }
    end

    def sum_crashes_with_map_reduce(make)
      puts "Counting all crashes of '#{make}' using Map/Reduce"
      opts = {
          :query => {:Make => make},
          :out => {:inline => true},
          :raw => true
      }
      with_connection { |coll| coll.map_reduce(@map, @reduce, opts) }
    end

    # Runs about 3 times faster than the map/reduce version.
    # The result is sorted by the number of crashes.
    def group_with_builtin
      puts "Grouping by 'Make' (built-in)"
      with_connection do |coll|
        coll.group({
          :key      => [:Make],
          :initial  => {:crashes => 0},
          :reduce   => 'function(doc, prev) {prev.crashes += 1;}'
        })
      end
    end

    # The result is sorted by Make.
    def group_with_map_reduce(limit = 0)
      puts "Grouping by 'Make' (map/reduce)"
      opts = {
          :limit => limit.to_i,
          :out => {:inline => true},
          :raw => true
      }
      with_connection { |coll| coll.map_reduce(@map, @reduce, opts) }
    end

  end
end
