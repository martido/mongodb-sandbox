#!/usr/bin/env ruby

require 'rubygems'
require 'mongo'
require 'benchmark'
require 'date'

class Setup

  def initialize(data, db, coll)
    @data = data
    @coll = Mongo::Connection.new.db(db).collection(coll)
  end

  def run

    # Unzip the input file if necessary.
    filename = if(File.extname(@data) == ".gz")
      gunzip(@data)
      @data.chomp(".gz")
    else
      @data
    end

    import_data(filename)
    create_indexes
    create_views
  end

  private

    def import_data(filename)

      docs = []
      checkpoint = 1000
      n = 0
      start = Time.now

      # Get the header from the data file.
      keys = File.open(filename, "r") do |file|
        file.first.chomp.split("|")
      end

      File.open(filename, "r") do |file|
        file.each_with_index do |row,index|
          next if index == 0
          values = row.split("|")
          docs << create_doc(keys, values)

          # Batch insert the created documents.
          if(docs.length % checkpoint == 0)
            insert_docs(docs)
            n += docs.length
            docs = []
          end
        end
      end

      # Insert the remaining documents.
      insert_docs(docs)
      n += docs.length

      # Print some statistics.
      delta = Time.now() - start
      printf("Imported: %d document(s) in %.3f seconds\n", n, delta)
      printf("Rate: %.3f document(s) / second\n", n/delta)
    end

    def create_indexes
      puts 'Creating indexes ...'
      @coll.create_index([['Make', Mongo::DESCENDING]])
    end

    def create_views
      Dir.glob("#{File.dirname(__FILE__)}/*.mr").sort.each do |file_name|
        view_name = File.basename(file_name).chomp('.mr')
        contents = File.open(file_name, 'rb').read
        match_data = /MAP <<\s(.*?)>>.*REDUCE <<\s(.*?)>>/m.match(contents)
        map, reduce = match_data.to_a[1,2]
        create_view(view_name, map, reduce)
      end
    end

    def create_view(view_name, map, reduce)
      opts = { :out => view_name}
      puts "Creating view #{view_name} ..."
      puts "Options: #{opts.inspect}"
      time = Benchmark.measure do
        @coll.map_reduce(map, reduce, opts)
      end
      printf("Created view in %.3f seconds\n", time.total)
    end

    def gunzip(filename)
      puts "Gunzipping #{filename} ..."
      command = "gunzip --force #{filename}"
      success = system(command)
      success && $?.exitstatus == 0
    end

    def create_doc(keys, values)
      doc = Hash[*keys.zip(values).flatten]

      # Convert some date strings to a Ruby Time object such that
      # the MongoDB driver is able to insert the correct BSON
      # type. Format: mm/dd/yyyy

      date = if RUBY_VERSION =~ /1\.9/
        # Switch mm and dd because Ruby 1.9 apparently changed the
        # expected pattern for Date.parse().
        mm,dd,yyyy = doc['Event Date'].split('/')
        Date.parse("#{dd}/#{mm}/#{yyyy}")
      else
        Date.parse(doc['Event Date'])
      end
      doc['Event Date'] = Time.utc(date.year, date.month, date.day)
      doc
    end

    def insert_docs(docs)
      puts "Inserting #{docs.length} document(s) ..."
      @coll.insert(docs)
    end

end

data  = ARGV[0]
db    = ARGV[1]
coll  = ARGV[2]

puts "Using Ruby version '#{RUBY_VERSION}'"
puts "Importing '#{data}' to #{db}@#{coll}"

Setup.new(data, db, coll).run
