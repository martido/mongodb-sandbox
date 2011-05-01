module Aviation
  module View

    # A somewhat failed attempt to use a pre-computed view (of both the number
    # of accidents and the total number of fatalities grouped by year and month)
    # to execute time-based Map/Reduce queries.
    #
    # A document looks like this (months are zero-based):
    # {
    #    "_id" : {
    #            "year" : 1994,
    #            "month" : 0
    #    },
    #    "value" : {
    #            "crashes" : 116,
    #            "fatalities" : 60
    #    }
    # }
    #
    # Showing all documents grouped by year and/or month as well as limiting the
    # ouput to a specific range of years is easy and faster than map/reducing on
    # the entire data set. However, all information about time is lost, because
    # 'year' and 'month' are just numbers. Limiting the output to a specific
    # range of years and months is now a lot more difficult.
    class EventDate < Aviation::Crashes

      def initialize(database_name, collection_name)
        super(database_name, collection_name)

        @map = <<-EOS.undent
          function() {
            emit(@KEY@, {
              crashes: this.value.crashes,
              fatalities: this.value.fatalities
            });
          }
        EOS

        @reduce = <<-EOS.undent
          function(key, values) {
            var crashes = 0;
            var fatalities = 0;
            values.forEach(function(doc) {
              crashes += doc.crashes;
              fatalities += doc.fatalities;
            });
            return {crashes: crashes, fatalities: fatalities};
          }
        EOS

        @emit_year = '{year: this._id.year}'
        @emit_null = '{year: null}'
      end

      def group_by_year_and_month
        with_connection { |coll| coll.find({}) }
      end

      def group_by_year
        map = @map.sub('@KEY@', @emit_year)
        opts = {
          :out => {:inline => true},
          :raw => true
        }
        with_connection { |coll| coll.map_reduce(map, @reduce, opts) }
      end

      def group_by_year_limit_to_range(from, to)
        map = @map.sub('@KEY@', @emit_year)
        opts = {
          :query => {
            "_id.year" => {
              '$gte' => from.year,
              '$lte' => to.year
            }
          },
          :out => {:inline => true},
          :raw => true
        }
        puts "Options: #{opts.inspect}"
        with_connection { |coll| coll.map_reduce(map, @reduce, opts) }
      end

      def group_by_null
        map = @map.sub('@KEY@', @emit_null)
        opts = {
          :out => {:inline => true},
          :raw => true
        }
        with_connection { |coll| coll.map_reduce(map, @reduce, opts) }
      end

      # The view needs to be updated as new crash reports are coming in.
      def update_view(doc)

        time = Time.at(doc['Event Date'])
        with_connection do |coll|
          coll.update(
            # Selector
            {
              :_id => {
                :year => time.year,
                :month => time.month - 1
              }
            },
            # Document
            {
              "$inc" => {
                'value.crashes' => 1,
                'value.fatalities' => doc['Total Fatal Injuries'].to_i
                }
            },
            # Options
            {
              :upsert => true
            }
          )
        end
      end

    end
  end
end
