require 'lib/aviation'

#crashes = Aviation::Crashes.new('aviation', 'crashes')
#crashes.find_by_range(Time.utc(1994,1,1), Time.utc(1994,1,2))

#make = Aviation::Make.new('aviation', 'crashes')
#make.find_all(10)
#make.find_by_make("CESSNA", 10)
#make.sum_crashes_with_builtin('Boeing')
#make.sum_crashes_with_map_reduce('Boeing')
#make.group_with_builtin
#make.group_with_map_reduce(10)

#ed = Aviation::EventDate.new('aviation', 'crashes')
#ed.group_by_year_and_month
#ed.group_by_year_and_month_limit_to_make('Boeing')
#ed.group_by_year_and_month_limit_to_range(Time.utc(1994,1), Time.utc(1994,2))
#ed.group_by_year_and_month_limit_to_range(Time.utc(1994), Time.utc(1995))
#ed.group_by_year
#ed.group_by_year_limit_to_make('Boeing')
#ed.group_by_null

#edv = Aviation::View::EventDate.new('aviation', 'event_date')
#edv.group_by_year_and_month
#edv.group_by_year
#edv.group_by_year_limit_to_range(Time.utc(1994), Time.utc(1995))
#edv.group_by_null
#edv.update_view({
#    "Event Date" => Time.utc(2011, 4, 29),
#    "Total Fatal Injuries" => "10"
#})
