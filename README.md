# CRUD Driver Spec Helpers

This gem can be used by developers of Foobara CRUD drivers to ensure adherence to the CRUD Driver interface.

## Installation

Add `gem "foobara-crud-driver-spec-helpers"` to the :test group of your Gemfile.

## Usage

In a spec file, you can do something like:

```ruby
require "foobara/spec_helpers/it_behaves_like_a_crud_driver"

RSpec.describe Foobara::SomeCrudDriver do
  after { Foobara.reset_alls }

  let(:crud_driver) { described_class.new }

  before do
    Foobara::Persistence.default_crud_driver = crud_driver
  end

  it_behaves_like_a_crud_driver
end
```

NOTE: If you are using a crud driver that requires strong typing, like the foobara-postgresql-crud-driver,
you will need to create the tables needed in a `before` block. See that gem's spec for an example.

## Contributing

Bug reports and pull requests are welcome on GitHub
at https://github.com/foobara/crud-driver-spec-helpers

## License

This project is licensed under the MPL-2.0 license. Please see LICENSE.txt for more info.
