require_relative '../test_helper'
require 'gelf'

class GelfInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  PORT = 12345
  BASE_CONFIG = %[
    port #{PORT}
    protocol_type udp
    tag gelf
  ]
  CONFIG = BASE_CONFIG + %!
    bind 127.0.0.1
  !
  IPv6_CONFIG = BASE_CONFIG + %!
    bind ::1
  !
  NO_TRUST_CLIENT_CONFIG = CONFIG + %!
    trust_client_timestamp false
  !
  STRIP_LEADING_UNDERSCORE_CONFIG = CONFIG + %!
    strip_leading_underscore false
  !

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::GelfInput).configure(conf)
  end

  def test_configure
    configs = {'127.0.0.1' => CONFIG}
    configs.merge!('::1' => IPv6_CONFIG) if ipv6_enabled?

    configs.each_pair { |k, v|
      driver = create_driver(v)
      assert_equal PORT, driver.instance.port
      assert_equal k, driver.instance.bind
      assert_equal 'json', driver.instance.parser_configs.first["@type"]
    }
  end

  def test_ignore_client_provided_time
    configs = {'127.0.0.1' => NO_TRUST_CLIENT_CONFIG}
    configs.each_pair { |k, v|
      driver = create_driver(v)

      tests = [
        {:short_message => 'short message', :full_message => 'time_t only', :timestamp => 12345678},
        {:short_message => 'short message', :full_message => 'no time'}
      ]

      driver.run(expect_emits: 2)  do
        notifier = GELF::Notifier.new(k, PORT)

        tests.each { |test|
          notifier.notify!(test)
        }
      end

      emits = driver.events
      assert_equal tests.length, emits.length, 'missing emitted events'
      # 0=tag; 1=fluent metadata time; 2=fluent message payload
      emits.each_index { |i|
        # has correct tag
        assert_equal 'gelf', emits[i][0]
        # has correct fluent metadata (Fluent::EventTime) time
        assert_not_equal tests[i][:timestamp].to_f, emits[i][1].to_f
        # has correct message data, short_message key
        assert_equal tests[i][:short_message], emits[i][2]['short_message']
        # has correct message data, full message key
        assert_equal tests[i][:full_message], emits[i][2]['full_message']
      }
    }
  end

  def test_parse_time
    # parse client timestamps
    configs = {'127.0.0.1' => CONFIG}
    # gelf-rb currently does not support IPv6 over UDP
    # configs.merge!('::1' => IPv6_CONFIG) if ipv6_enabled?

    configs.each_pair { |k, v|
      driver = create_driver(v)

      # the string timestamps probably should not work but do
      tests = [
        {:short_message => 'short message', :full_message => 'no time'},
        {:short_message => 'short message', :full_message => 'time_t only', :timestamp => 1234567890},
        {:short_message => 'short message', :full_message => 'gelf spec time', :timestamp => 1234567890.1234},
        {:short_message => 'short message', :full_message => 'high precision time', :timestamp => 1234567890.1234567},
        {:short_message => 'short message', :full_message => 'future precision time', :timestamp => 12345678901.1234567},
        {:short_message => 'short message', :full_message => 'future time_t only', :timestamp => 12345678901},
        {:short_message => 'short message', :full_message => 'string time_t', :timestamp => "1234567890"},
        {:short_message => 'short message', :full_message => 'string gelf spec time', :timestamp => "1234567890.1234"}
      ]

      driver.run(expect_emits: 2)  do
        notifier = GELF::Notifier.new(k, PORT)

        tests.each { |test|
          notifier.notify!(test)
        }
      end

      emits = driver.events
      assert_equal tests.length, emits.length, 'missing emitted events'
      # 0=tag; 1=fluent metadata time; 2=fluent message payload
      emits.each_index { |i|
        # has correct tag
        assert_equal 'gelf', emits[i][0]
        # has correct fluent metadata (Fluent::EventTime) time
        assert_equal tests[i][:timestamp].to_f, emits[i][1].to_f unless tests[i][:timestamp].nil?
        # has correct message data, short_message key
        assert_equal tests[i][:short_message], emits[i][2]['short_message']
        # has correct message data, full_message key
        assert_equal tests[i][:full_message], emits[i][2]['full_message']
      }
    }
  end

  def test_bogus_timestamps
    # should accept messages with bogus times but ignore offered timestamps (again, do not run in 1970)
    configs = {'127.0.0.1' => CONFIG}
    # gelf-rb currently does not support IPv6 over UDP
    # configs.merge!('::1' => IPv6_CONFIG) if ipv6_enabled?

    configs.each_pair { |k, v|
      driver = create_driver(v)

      # please note -- negative time_t values will be "correctly" handled by this but probably should cause 
      # an error; negative time_t + nsec fail only because the parent fluent will add not subtract the nsec
      # value
      # {:short_message => 'short message', :full_message => 'negative time_t', :timestamp => -1234567890},

      tests = [
        {:short_message => 'short message', :full_message => 'negative high resolution time_t', :timestamp => -1234567890.123},
        {:short_message => 'short message', :full_message => 'left string time_t', :timestamp => "S1234567890"},
        {:short_message => 'short message', :full_message => 'right string time_t', :timestamp => "1234567890S"},
        {:short_message => 'short message', :full_message => 'left string nsec', :timestamp => "1234567890.S123"},
        {:short_message => 'short message', :full_message => 'right string nsec', :timestamp => "1234567890.123S"},
        {:short_message => 'short message', :full_message => 'super crazy', :timestamp => "S1234567890S.S1234567S"},
        {:short_message => 'short message', :full_message => 'future time_t only', :timestamp => "BAD TIME"},
        {:short_message => 'short message', :full_message => 'thai time_t', :timestamp => "ณ123456789"}
      ]

      driver.run(expect_emits: 2)  do
        notifier = GELF::Notifier.new(k, PORT)

        tests.each { |test|
          notifier.notify!(test)
        }
      end

      emits = driver.events
      assert_equal tests.length, emits.length, 'missing emitted events'
      # 0=tag; 1=fluent metadata time; 2=fluent message payload
      emits.each_index { |i|
        # has correct tag
        assert_equal 'gelf', emits[i][0]
        # has correct fluent metadata (Fluent::EventTime) time
        assert_not_equal tests[i][:timestamp].to_f, emits[i][1].to_f
        # has correct message data, short_message key
        assert_equal tests[i][:short_message], emits[i][2]['short_message']
        # has correct message data, full_message key
        assert_equal tests[i][:full_message], emits[i][2]['full_message']
      }
    }
  end

  def test_strip_leading_underscore
    configs = {'127.0.0.1' => CONFIG}
    # gelf-rb currently does not support IPv6 over UDP
    # configs.merge!('::1' => IPv6_CONFIG) if ipv6_enabled?

    configs.each_pair { |k, v|
      driver = create_driver(v)

      tests = [
        {:given =>
         {
           :timestamp => 12345,
           :short_message => 'short message',
           :full_message => 'full message',
           '_custom_field_one' => 12345,
           '__custom_field_two' => 'more data'
         },
         :expected =>
         {
           'short_message' => 'short message',
           'full_message' => 'full message',
           'custom_field_one' => 12345,
           '_custom_field_two' => 'more data'
         }
        }
      ]

      driver.run(expect_emits: 1) do
        notifier = GELF::Notifier.new(k, PORT)

        tests.each { |test|
          notifier.notify!(test[:given])
        }
      end

      emits = driver.events
      assert_equal tests.length, emits.length, 'missing emitted events'
      emits.each_index { |i|
        assert_equal 'gelf', emits[i][0]
        assert_equal tests[i][:timestamp].to_f, emits[i][1].to_f unless tests[i][:timestamp].nil?
        assert_block "expectation not met: #{tests[i][:expected]}" do
          emits[i][2].merge(tests[i][:expected]) == emits[i][2]
        end
      }
    }
  end

  def test_leave_leading_underscore
    configs = {'127.0.0.1' => STRIP_LEADING_UNDERSCORE_CONFIG}
    # gelf-rb currently does not support IPv6 over UDP
    # configs.merge!('::1' => IPv6_CONFIG) if ipv6_enabled?

    configs.each_pair { |k, v|
      driver = create_driver(v)

      tests = [
        {:given =>
         {
           :timestamp => 12345,
           :short_message => 'short message',
           :full_message => 'full message',
           '_custom_field_one' => 12345,
           '__custom_field_two' => 'more data'
         },
         :expected =>
         {
           'short_message' => 'short message',
           'full_message' => 'full message',
           '_custom_field_one' => 12345,
           '__custom_field_two' => 'more data'
         }
        }
      ]

      driver.run(expect_emits: 1) do
        notifier = GELF::Notifier.new(k, PORT)

        tests.each { |test|
          notifier.notify!(test[:given])
        }
      end

      emits = driver.events
      assert_equal tests.length, emits.length, 'missing emitted events'
      emits.each_index { |i|
        assert_equal 'gelf', emits[i][0]
        assert_equal tests[i][:timestamp].to_f, emits[i][1].to_f unless tests[i][:timestamp].nil?
        assert_block "expectation not met: #{tests[i][:expected]}" do
          emits[i][2].merge(tests[i][:expected]) == emits[i][2]
        end
      }
    }
  end

end
