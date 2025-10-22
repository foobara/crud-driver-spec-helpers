require "base64"

module RspecHelpers
  module ItBehavesLikeACrudDriver
    module ClassMethods
      def it_behaves_like_a_crud_driver
        define_method :table do
          entity_class.current_transaction_table.entity_attributes_crud_driver_table
        end

        let(:driver) { entity_class.entity_base.entity_attributes_crud_driver }
        let(:driver_class) { driver.class }

        describe ".has_real_transactions?" do
          it "is a boolean" do
            expect([true, false]).to include(described_class.has_real_transactions?)
          end
        end

        describe "#table_for" do
          context "when using a table_prefix" do
            let(:table_prefix) { "some_prefix" }
            let(:prefixed_crud_driver) { driver_class.new(table_prefix:) }
            let(:entity_class) do
              stub_module("SomeOrg") { foobara_organization! }
              stub_module("SomeOrg::SomeDomain") { foobara_domain! }
              stub_class("SomeOrg::SomeDomain::SomeEntity", Foobara::Entity) do
                attributes do
                  id :integer
                  foo :integer
                  bar :symbol
                  created_at :datetime, :allow_nil
                end

                primary_key :id
              end
            end

            it "includes the table_prefix in the table name" do
              table = prefixed_crud_driver.table_for(entity_class)
              expect(table.table_name).to eq("some_prefix_some_entity")
            end

            context "when table_prefix is true" do
              let(:table_prefix) { true }

              it "uses the org and domain as the table_prefix" do
                table = prefixed_crud_driver.table_for(entity_class)
                expect(table.table_name).to eq("some_org_some_domain_some_entity")
              end
            end
          end
        end

        context "tests from redis-crud-driver" do
          let(:entity_class) do
            stub_class("SomeEntity", Foobara::Entity) do
              attributes do
                id :integer
                foo :integer
                bar :symbol
                created_at :datetime, :allow_nil
              end

              primary_key :id
            end
          end

          describe ".transaction" do
            it "can create, load, and update records" do
              expect do
                entity_class.create(foo: 1, bar: :baz)
              end.to raise_error(Foobara::Persistence::EntityBase::Transaction::NoCurrentTransactionError)

              transaction = nil

              entity1 = entity_class.transaction do |tx|
                transaction = tx

                entity = entity_class.create(foo: 1, bar: :baz)

                expect(entity).to be_a(entity_class)
                expect(entity).to_not be_persisted
                expect(entity).to_not be_loaded

                expect(tx).to be_open
                expect(Foobara::Persistence.current_transaction(entity)).to be(tx)

                entity
              end

              expect(transaction).to be_closed
              expect(Foobara::Persistence.current_transaction(entity1)).to be_nil

              expect(entity1).to be_a(entity_class)
              expect(entity1).to be_persisted
              expect(entity1).to be_loaded

              entity_class.transaction do
                entity = entity_class.thunk(entity1.primary_key)

                expect(entity).to be_a(entity_class)
                expect(entity).to be_persisted
                expect(entity).to_not be_loaded

                expect(entity.bar).to eq(:baz)

                expect(entity).to be_loaded

                singleton = entity_class.thunk(entity.primary_key)
                expect(singleton).to be(entity)

                entity.bar = "bazbaz"
              end

              entity_class.transaction do
                entity = Foobara::Persistence.current_transaction(entity_class).load(entity_class,
                                                                                     entity1.primary_key)
                expect(entity.bar).to eq(:bazbaz)

                expect(entity_class.all.to_a).to eq([entity1])
              end
            end

            it "can rollback" do
              entity1 = entity_class.transaction do
                entity_class.create(foo: 10, bar: :baz)
              end

              entity_class.transaction do |tx|
                entity = entity_class.thunk(entity1.primary_key)
                expect(entity.foo).to eq(10)

                entity.foo = 20

                expect(entity.foo).to eq(20)

                begin
                  tx.rollback!
                rescue Foobara::Persistence::EntityBase::Transaction::RolledBack # rubocop:disable Lint/SuppressedException
                end

                expect(entity.foo).to eq(10)

                entity_class.transaction do
                  expect(entity_class.load(entity.primary_key).foo).to eq(10)
                end

                expect do
                  entity.foo = 20
                end.to raise_error(Foobara::Persistence::EntityBase::Transaction::NoCurrentTransactionError)
              end

              entity_class.transaction do |tx|
                entity = entity_class.load(entity1.primary_key)
                entity = entity_class.load(entity.primary_key)
                expect(entity.foo).to eq(10)

                entity.foo = 20

                expect(entity.foo).to eq(20)

                tx.flush!

                expect(entity.foo).to eq(20)

                entity.foo = 30

                tx.revert!

                expect(entity.foo).to eq(20)
              end

              entity_class.transaction do
                entity = entity_class.load(entity1.primary_key)
                expect(entity.foo).to eq(20)
              end
            end

            it "can hard delete" do
              entity_class.transaction do
                expect(entity_class.all.to_a).to be_empty
                entity = entity_class.create(foo: 10, bar: :baz)
                expect(entity_class.all.to_a).to eq([entity])
                entity.hard_delete!
                expect(entity_class.all.to_a).to be_empty
              end

              entity1 = entity_class.transaction do
                expect(entity_class.all.to_a).to be_empty
                entity_class.create(foo: 10, bar: :baz)
              end

              entity_class.transaction do
                entity = entity_class.thunk(entity1.primary_key)
                expect(entity.foo).to eq(10)

                entity.hard_delete!

                expect(entity).to be_hard_deleted

                # TODO: make this work without needing to call #to_a
                expect(entity_class.all.to_a).to be_empty

                expect do
                  entity.foo = 20
                end.to raise_error(Foobara::Entity::CannotUpdateHardDeletedRecordError)

                expect(entity.foo).to eq(10)

                entity.restore!
                expect(entity_class.all.to_a).to eq([entity])

                entity.foo = 20
              end

              entity_class.transaction do
                # TODO: make calling #to_a not necessary
                expect(entity_class.all.to_a).to eq([entity1])
                entity = entity_class.thunk(entity1.primary_key)

                expect(entity).to be_persisted
                expect(entity).to_not be_hard_deleted
                expect(entity.foo).to eq(20)

                entity.hard_delete!

                expect(entity_class.all.to_a).to be_empty
                expect(entity).to be_hard_deleted
              end

              entity_class.transaction do
                expect do
                  entity_class.load(entity1.primary_key)
                end.to raise_error(Foobara::Entity::NotFoundError)

                expect(entity_class.all.to_a).to be_empty
              end
            end

            describe "#hard_delete_all" do
              it "deletes everything" do
                entities = []

                entity_class.transaction do
                  4.times do
                    entity = entity_class.create(foo: 1, bar: :baz)
                    entities << entity
                  end

                  # TODO: make calling #to_a not necessary
                  expect(entity_class.all.to_a).to eq(entities)
                end

                entity_ids = entities.map(&:primary_key)

                expect(entity_ids).to contain_exactly(1, 2, 3, 4)

                entity_class.transaction do
                  entities = []

                  entity_class.all do |record|
                    entities << record
                  end

                  entity_ids = entities.map(&:primary_key)

                  expect(entity_ids).to contain_exactly(1, 2, 3, 4)

                  4.times do
                    entity = entity_class.create(foo: 1, bar: :baz)
                    entities << entity
                  end

                  expect(entity_class.all).to match_array(entities)

                  Foobara::Persistence.current_transaction(entities.first).hard_delete_all!(entity_class)

                  expect(entities).to all be_hard_deleted
                  expect(entity_class.all.to_a).to be_empty
                end

                entity_class.transaction do
                  expect(entity_class.all.to_a).to be_empty
                end
              end
            end
          end

          describe "#load_many" do
            it "loads many" do
              entities = nil
              entity_ids = nil

              entity_class.transaction do |tx|
                [
                  { foo: 11, bar: :baz },
                  { foo: 22, bar: :baz },
                  { foo: 33, bar: :baz },
                  { foo: 44, bar: :baz }
                ].map do |attributes|
                  entity_class.create(attributes)
                end

                expect(entity_class.count).to eq(4)

                entity_class.transaction(mode: :use_existing) do
                  expect(entity_class.count).to eq(4)
                end

                tx2 = entity_class.transaction(mode: :use_existing)

                entity_class.entity_base.using_transaction(tx2) do
                  expect(entity_class.count).to eq(4)
                end

                expect(entity_class.count).to eq(4)
                entity_class.transaction(mode: :open_nested) do |tx|
                  # TODO: Why wouldn't this be 5????
                  expect(entity_class.count).to eq(0)
                  entity_class.create(foo: 55, bar: :baz)
                  expect(entity_class.count).to eq(1)
                  tx.rollback!
                end
                expect(entity_class.count).to eq(4)

                tx.flush!

                entity_class.transaction(mode: :use_existing) do
                  expect(entity_class.count).to eq(4)
                end

                entity_class.transaction(mode: :open_nested) do
                  expect(entity_class.count).to eq(4)
                end

                entities = entity_class.all

                expect(entities).to all be_a(Foobara::Entity)
                expect(entities.size).to eq(4)

                entity_ids = entities.map(&:primary_key)
                expect(entity_ids).to contain_exactly(1, 2, 3, 4)
              end

              entity_class.transaction do
                entity_class.load_many([entity_class.thunk(1)])
                loaded_entities = entity_class.load_many(entity_ids)
                expect(loaded_entities).to all be_loaded
                expect(loaded_entities).to eq(entities)
              end
            end
          end

          describe "#all_exist?" do
            it "answers whether they all exist or not" do
              entity_class.transaction do
                expect(entity_class.all_exist?([101, 102])).to be(false)

                [
                  { foo: 11, bar: :baz, id: 101 },
                  { foo: 22, bar: :baz, id: 102 },
                  { foo: 33, bar: :baz },
                  { foo: 44, bar: :baz }
                ].map do |attributes|
                  entity_class.create(attributes)
                end

                entity_class.all do |record|
                  expect(record).to_not be_persisted
                end

                expect(entity_class.all_exist?([101, 102])).to be(true)
                expect(entity_class.all_exist?([1, 2, 101, 102])).to be(false)
              end

              entity_class.transaction do
                expect(entity_class.all_exist?([1, 2, 101, 102])).to be(true)
                expect(entity_class.all_exist?([3])).to be(false)
              end
            end
          end

          describe "#unhard_delete!" do
            context "when record was dirty when hard deleted" do
              it "is still dirty" do
                entity = entity_class.transaction do
                  entity_class.create(foo: 11, bar: :baz)
                end

                entity_class.transaction do
                  entity = entity_class.thunk(entity.primary_key)

                  expect(entity).to be_persisted

                  expect(entity).to_not be_dirty

                  entity.foo = 12

                  expect(entity).to be_dirty
                  expect(entity).to_not be_hard_deleted

                  entity.foo = 11

                  expect(entity).to_not be_dirty
                  expect(entity).to_not be_hard_deleted

                  entity.foo = 12

                  expect(entity).to be_dirty
                  expect(entity).to_not be_hard_deleted

                  entity.hard_delete!

                  expect(entity).to be_dirty
                  expect(entity).to be_hard_deleted

                  entity.unhard_delete!

                  expect(entity).to be_dirty
                  expect(entity).to_not be_hard_deleted
                end
              end
            end
          end

          describe "#exists?" do
            it "answers it exists or not" do
              entity_class.transaction do
                expect(entity_class.all_exist?([101, 102])).to be(false)

                entity_class.create(foo: 11, bar: :baz, id: 101)

                expect(entity_class.exists?(101)).to be(true)

                entity_class.create(foo: 11, bar: :baz)

                expect(entity_class.exists?(1)).to be(false)
              end

              entity_class.transaction do
                expect(entity_class.exists?(101)).to be(true)

                expect(entity_class.exists?(1)).to be(true)
                expect(entity_class.exists?(2)).to be(false)
              end
            end
          end

          context "when creating a record with an already-in-use key" do
            it "explodes" do
              entity_class.transaction do
                entity_class.create(foo: 11, bar: :baz, id: 101)
              end

              expect do
                entity_class.transaction do
                  entity_class.create(foo: 11, bar: :baz, id: 101)
                end
              end.to raise_error(Foobara::Persistence::EntityAttributesCrudDriver::Table::CannotInsertError)
            end
          end

          context "when restoring with a created record" do
            it "hard deletes it" do
              entity_class.transaction do |tx|
                record = entity_class.create(foo: 11, bar: :baz, id: 101)

                tx.revert!

                expect(record).to be_hard_deleted
              end

              entity_class.transaction do
                expect(entity_class.count).to eq(0)
              end
            end
          end

          context "when persisting entity with an association" do
            let(:aggregate_class) do
              entity_class
              some_model_class

              stub_class "SomeAggregate", Foobara::Entity do
                attributes do
                  id :integer
                  foo :integer
                  some_model SomeModel, :required
                  some_entities [SomeEntity]
                end

                primary_key :id
              end
            end

            let(:some_model_class) do
              some_other_entity_class

              stub_class "SomeModel", Foobara::Model do
                attributes do
                  some_other_entity SomeOtherEntity, :required
                end
              end
            end

            let(:some_other_entity_class) do
              stub_class "SomeOtherEntity", Foobara::Entity do
                attributes do
                  id :integer
                  foo :integer, :required
                end

                primary_key :id
              end
            end

            it "writes the records to disk using primary keys" do
              some_entity2 = nil

              some_entity1 = aggregate_class.transaction do
                some_entity2 = entity_class.create(foo: 11, bar: :baz, created_at: Time.now)
                entity_class.create(foo: 11, bar: :baz, id: 101)
              end

              some_other_entity = nil

              entity_class.transaction do
                some_entity3 = entity_class.create(foo: 11, bar: :baz, id: 102)
                some_entity4 = entity_class.create(foo: 11, bar: :baz)
                some_other_entity = SomeOtherEntity.create(foo: 11)

                some_model = SomeModel.new(some_other_entity:)

                aggregate_class.create(
                  foo: 30,
                  some_model:,
                  some_entities: [
                    1,
                    some_entity1,
                    some_entity3,
                    some_entity4
                  ]
                )
              end

              entity_class.transaction do |tx|
                crud_table = aggregate_class.current_transaction_table.entity_attributes_crud_driver_table
                raw_records = crud_table.all.to_a
                expect(raw_records.size).to eq(1)
                raw_record = raw_records.first

                record = aggregate_class.build(raw_record)
                expect(record.some_entities.map(&:id)).to contain_exactly(1, 2, 101, 102)
                expect(record.some_model.some_other_entity.id).to eq(some_other_entity.id)

                loaded_aggregate = aggregate_class.load(1)
                expect(loaded_aggregate.some_entities).to all be_a(SomeEntity)
                expect(loaded_aggregate.some_entities.map(&:primary_key)).to contain_exactly(1, 2, 101, 102)

                new_aggregate = aggregate_class.create(
                  foo: "30",
                  some_entities: [
                    entity_class.create(foo: 11, bar: :baz)
                  ],
                  some_model: {
                    some_other_entity: {
                      foo: 10
                    }
                  }
                )

                expect(new_aggregate.some_model.some_other_entity.foo).to eq(10)

                expect(aggregate_class.contains_associations?).to be(true)
                expect(entity_class.contains_associations?).to be(false)

                tx.flush!

                record = SomeAggregate.load(new_aggregate.primary_key)
                expect(record.foo).to eq(30)
                expect(record.some_entities.map(&:id)).to contain_exactly(new_aggregate.some_entities.first.primary_key)
                expect(record.some_model.some_other_entity.id).to eq(
                  new_aggregate.some_model.some_other_entity.primary_key
                )
                expect(record.primary_key).to eq(new_aggregate.primary_key)
              end
            end
          end

          describe "#all" do
            context "when using string ids" do
              let(:entity_class) do
                stub_class("SomeEntityStringId", Foobara::Entity) do
                  attributes do
                    id :string, :required
                    foo :integer, :required
                    bar :integer, :required
                  end

                  primary_key :id
                end
              end

              context "when there's tons of records existing" do
                let(:records) do
                  all = []

                  entity_class.transaction do
                    200.times do |i|
                      id = Base64.strict_encode64(SecureRandom.random_bytes(20))
                      all << entity_class.create(id:, foo: i, bar: i)
                    end
                  end

                  all
                end

                it "returns all of the expected records" do
                  expect(records.size).to eq(200)

                  all = []

                  entity_class.transaction do
                    entity_class.all do |record|
                      all << record
                    end
                  end

                  expect(all.size).to eq(records.size)
                end
              end
            end
          end
          describe "#truncate" do
            it "deletes everything" do
              entity_class.transaction do
                4.times do
                  entity_class.create(foo: 1, bar: :baz)
                end

                # TODO: make calling #to_a not necessary
                expect(entity_class.count).to eq(4)
              end

              entity_class.transaction do
                expect(entity_class.count).to eq(4)

                Foobara::Persistence.current_transaction(entity_class).truncate!

                expect(entity_class.count).to eq(0)
                expect(entity_class.all.to_a).to be_empty
              end

              entity_class.transaction do
                expect(entity_class.count).to eq(0)
                expect(entity_class.all.to_a).to be_empty
              end
            end
          end
        end

        # TODO: come up with better names that just where the tests were move from
        context "tests from postgresql-crud-driver" do
          let(:entity_class) do
            stub_class("SomeEntity", Foobara::Entity) do
              attributes do
                id :integer
                foo :integer
                bar :symbol
                created_at :datetime, default: -> { Time.now }
              end
              primary_key :id
            end
          end

          describe "#all" do
            it "yields all records" do
              entity_class.transaction do
                111.times do
                  entity_class.create(foo: 1, bar: :foo)
                end
              end

              entity_class.transaction do
                expect(table.all.to_a.size).to eq(111)
                expect(table.all(page_size: 10).to_a.size).to eq(111)
                expect(entity_class.all.first.foo).to eq(1)
              end
            end
          end

          describe "#insert" do
            it "inserts a record" do
              expect do
                entity_class.transaction do
                  entity_class.create(foo: 1, bar: :foo)
                end
              end.to change {
                entity_class.transaction { entity_class.count }
              }.from(0).to(1)
            end
          end

          describe "#find" do
            it "can find a record" do
              created_record = entity_class.transaction do
                entity_class.create(foo: 1, bar: :foo)
              end

              record_id = created_record.id

              entity_class.transaction do |tx|
                attributes = tx.table_for(entity_class).entity_attributes_crud_driver_table.find(record_id)
                expect(attributes[:foo]).to eq(1)
                record = entity_class.load(record_id)

                expect(record).to be_a(entity_class)
                expect(record.id).to eq(record_id)
                expect(record.foo).to eq(1)
                expect(record.bar).to eq(:foo)
                expect(record.created_at).to be_a(Time)
              end
            end
          end

          describe "#update" do
            it "can update a record" do
              created_record = entity_class.transaction do
                entity_class.create(foo: 1, bar: :foo)
              end

              record_id = created_record.id

              entity_class.transaction do
                record = entity_class.load(record_id)
                record.foo = 2
              end

              record = entity_class.transaction do
                entity_class.load(record_id)
              end

              expect(record.foo).to eq(2)
            end
          end

          describe "#hard_delete" do
            it "can delete a record" do
              created_record = entity_class.transaction do
                entity_class.create(foo: 1, bar: :foo)
                entity_class.create(foo: 2, bar: :baz)
              end

              record_id = created_record.id

              expect do
                entity_class.transaction do
                  record = entity_class.load(record_id)
                  record.hard_delete!
                end
              end.to change {
                entity_class.transaction { entity_class.count }
              }.from(2).to(1)
            end
          end

          describe "#hard_delete_all" do
            it "deletes all records" do
              entity_class.transaction do
                entity_class.create(foo: 1, bar: :foo)
                entity_class.create(foo: 2, bar: :baz)
              end

              expect do
                entity_class.transaction do
                  table.hard_delete_all
                end
              end.to change {
                entity_class.transaction { entity_class.count }
              }.from(2).to(0)
            end
          end
        end

        context "tests from in-memory-crud-driver" do
          let(:entity_class) do
            stub_class "Details", Foobara::Model do
              attributes do
                name :string, :required
              end
            end

            stub_class "Item", Foobara::Entity do
              attributes do
                id :integer
                details Details, :required
              end

              primary_key :id
            end

            stub_class "YetAnotherEntity", Foobara::Entity do
              attributes do
                pk :integer
                foo :integer
                bar :symbol
                stuff :allow_nil do
                  items [Item]
                end
              end

              primary_key :pk
            end
          end

          describe "#find_by/#find_many_by" do
            it "can find by an attribute" do
              entity1 = entity2 = entity3 = entity4 = nil

              entity_class.transaction do
                entity1 = entity_class.create(foo: 11, bar: :baz)
                entity2 = entity_class.create(foo: 22, bar: :baz)
                entity3 = entity_class.create(foo: 33, bar: :basil)
                entity4 = entity_class.create(foo: 44, bar: :basil)

                # non-persisted records
                expect(entity_class.find_by(foo: "22")).to eq(entity2)
                expect(entity_class.find_many_by(foo: "11").to_a).to eq([entity1])
                expect(entity_class.find_many_by(bar: "basil").to_a).to eq([entity3, entity4])
              end

              entity8 = nil

              entity_class.transaction do
                entity5 = entity_class.create(foo: 55, bar: :baz)
                entity6 = entity_class.create(foo: 66, bar: :baz)
                entity7 = entity_class.create(foo: 77, bar: :basil)
                entity8 = entity_class.create(foo: 88, bar: :basil)

                # mixture of persisted and non-persisted records
                expect(entity_class.find_by(foo: "22")).to eq(entity2)
                expect(entity_class.find_by(foo: "55")).to eq(entity5)

                expect(table.find_by(foo: "22")[:pk]).to eq(entity2.pk)

                expect(entity_class.find_many_by(foo: "11").to_a).to eq([entity1])
                expect(entity_class.find_many_by(foo: "66").to_a).to eq([entity6])
                expect(entity_class.find_many_by(bar: "basil").to_a).to eq([entity7, entity8, entity3, entity4])
              end

              item = nil

              entity_class.transaction do
                item = Item.create(details: { name: "foo" })
                entity8 = entity_class.load(entity8.pk)
                entity8.stuff = { items: [item] }

                expect(entity_class.find_by(stuff: { items: [item] })).to eq(entity8)
                expect(Item.find_by(details: Details.new(name: "foo"))).to eq(item)
              end

              entity_class.transaction do
                expect(entity_class.find_by(stuff: { items: [item] })).to eq(entity8)
                expect(Item.find_by(details: Details.new(name: "foo"))).to eq(item)
              end
            end
          end

          describe ".has_real_transactions?" do
            it "is a boolean" do
              expect(described_class.has_real_transactions?).to(satisfy { |v| [true, false].include?(v) })
            end
          end
        end

        context "tests from local-files-crud-driver" do
          let(:capybara_class) do
            stub_class "Capybara", Foobara::Entity do
              attributes do
                id :integer
                name :string, :required
                age :integer, :required
                date_stuff do
                  birthdays [:date]
                  created_at :datetime
                end
              end

              primary_key :id
            end
          end

          it "can persist records" do
            capybara_class

            fumiko = nil
            Capybara.transaction do
              fumiko = Capybara.create(name: "Fumiko", age: 100,
                                       date_stuff: { birthdays: [Date.today], created_at: Time.now })
              Capybara.create(name: "Barbara", age: 200, date_stuff: { birthdays: [Date.today], created_at: Time.now })
              Capybara.create(name: "Basil", age: 300, date_stuff: { birthdays: [Date.today], created_at: Time.now })
            end

            capybaras = Capybara.transaction do
              Capybara.all
            end

            expect(capybaras.map(&:name)).to match_array(["Fumiko", "Barbara", "Basil"])

            Capybara.transaction do
              fumiko = Capybara.load(fumiko.id)
              expect(fumiko.age).to eq(100)
              fumiko.age += 1
            end

            Capybara.transaction do
              fumiko = Capybara.load(fumiko.id)
              expect(fumiko.age).to eq(101)
              expect(Capybara.count).to eq(3)
            end

            Capybara.transaction do
              fumiko = Capybara.load(fumiko.id)
              fumiko.hard_delete!
              expect(Capybara.count).to eq(2)
            end

            Capybara.transaction do
              expect(Capybara.count).to eq(2)
            end

            Capybara.transaction do |tx|
              tx.hard_delete_all!(Capybara)
              expect(Capybara.count).to eq(0)
            end

            Capybara.transaction do
              expect(Capybara.count).to eq(0)
            end
          end
        end
      end
    end
  end
end

RSpec.configure do |c|
  c.extend RspecHelpers::ItBehavesLikeACrudDriver::ClassMethods
end
