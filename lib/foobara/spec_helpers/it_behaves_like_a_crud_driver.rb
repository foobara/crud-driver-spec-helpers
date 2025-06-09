module RspecHelpers
  module ItBehavesLikeACrudDriver
    module ClassMethods
      def it_behaves_like_a_crud_driver
      end
    end
  end
end

RSpec.configure do |c|
  c.extend RspecHelpers::ItBehavesLikeACrudDriver::ClassMethods
end
