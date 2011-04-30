module Aviation
  module CoreExtensions
    module String
      module Util
        # Taken from the Homebrew source base.
        def undent
          gsub(/^.{#{slice(/^ +/).length}}/, '')
        end
      end
    end
  end
end
