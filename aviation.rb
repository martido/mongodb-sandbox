$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

require 'rubygems'
require 'mongo'

require 'aviation/crashes'
require 'aviation/event_date'
require 'aviation/view/event_date'
require 'aviation/make'
require 'aviation/core_ext/string'

module Aviation

  KEYS = [
         "Event Id",
         "Investigation Type",
         "Accident Number",
         "Event Date",
         "Location",
         "Country",
         "Latitude",
         "Longitude",
         "Airport Code",
         "Airport Name",
         "Injury Severity",
         "Aircraft Damage",
         "Aircraft Category",
         "Registration Number",
         "Make",
         "Model",
         "Amateur Built",
         "Number of Engines",
         "Engine Type",
         "FAR Description",
         "Schedule",
         "Purpose of Flight",
         "Air Carrier",
         "Total Fatal Injuries",
         "Total Serious Injuries",
         "Total Minor Injuries",
         "Total Uninjured",
         "Weather Condition",
         "Broad Phase of Flight",
         "Report Status",
         "Publication Date"
        ]

end
