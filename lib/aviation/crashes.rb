require 'benchmark'

module Aviation
  class Crashes

    def initialize(database_name, collection_name)
      @database_name = database_name
      @collection_name = collection_name
    end

    def find_by_id(id)
      puts "Finding crash with ID #{id}"
      with_connection do |coll|
        coll.find_one(:_id => BSON::ObjectId.from_string(id))
      end
    end

    def find_by_range(from, to)
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
      with_connection { |coll| coll.find(opts) }
    end

    def with_connection
      time = Benchmark.measure do
        begin
          puts "Opening connection to MongoDB ..."
          conn = Mongo::Connection.new
          db = conn.db(@database_name)
          coll = db.collection(@collection_name)
          out(yield coll)
        ensure
          puts "Closing connection to MongoDB ..."
          conn.close
        end
      end
      printf("Time: %.3f\n", time.total)
    end

    def out(result)
      case result
        when Mongo::Collection
          result.find().each { |doc| p doc }
        when Mongo::Cursor, Array
          result.each { |doc| p doc }
          result.close if result.respond_to?(:close)
        else
          p result
      end
    end

  end
end
