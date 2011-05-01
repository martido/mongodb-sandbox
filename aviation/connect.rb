require 'benchmark'

def with_connection(database_name, collection_name)
  time = Benchmark.measure do
    begin
      puts "Opening connection to MongoDB ..."
      conn = Mongo::Connection.new
      db = conn.db(database_name)
      coll = db.collection(collection_name)
      print(yield coll)
    ensure
      puts "Closing connection to MongoDB ..."
      conn.close
    end
  end
  printf("Time: %.3f\n", time.total)
end

private

  def print(result)
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
