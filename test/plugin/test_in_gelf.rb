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
    client_timestamp_to_i false
    trust_client_timestamp true
    tag gelf
  ]
  CONFIG = BASE_CONFIG + %!
    bind 127.0.0.1
  !
  IPv6_CONFIG = BASE_CONFIG + %!
    bind ::1
  !

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::GelfInput).configure(conf)
  end

  def test_configure
    configs = {'127.0.0.1' => CONFIG}
    configs.merge!('::1' => IPv6_CONFIG) if ipv6_enabled?

    configs.each_pair { |k, v|
      driver = create_driver(v)
      assert_equal PORT,driver.instance.port
      assert_equal k,driver.instance.bind
      assert_equal 'json',driver.instance.parser_configs.first["@type"]
    }
  end

  def test_parse
    configs = {'127.0.0.1' => CONFIG}
    # gelf-rb currently does not support IPv6 over UDP
    # configs.merge!('::1' => IPv6_CONFIG) if ipv6_enabled?

    configs.each_pair { |k, v|
      driver = create_driver(v)

      tests = [
        {:short_message => 'short message', :full_message => 'no time'},
        {:short_message => 'short message', :full_message => 'int time', :timestamp => 12345678},
        {:short_message => 'short message', :full_message => 'float time', :timestamp => 12345678.1},
        {:short_message => 'short message', :full_message => 'gelf time', :timestamp => 12345678.1234},
        {:short_message => 'short message', :full_message => 'high resolution time', :timestamp => 12345678.1234567},
        {:short_message => 'short message', :full_message => 'future time', :timestamp => 123456789.1234},
        {:short_message => 'short message', :full_message => 'past time', :timestamp => -123456789.1234}
      ]

     driver.run(expect_emits: 2)  do
        notifier = GELF::Notifier.new(k, PORT)

        tests.each { |test|
          notifier.notify!(test)
        }
      end

      emits =driver.events
      assert_equal tests.length, emits.length, 'missing emitted events'
      # emits: fluent tag, fluent time, message hash
      emits.each_index { |i|
        #assert_equal 'gelf', emits[tag]
        #assert_equal 'gelf', emits[i][0]
        #assert_equal tests[i][:short_message], emits[i][2]['short_message']
        puts emits[i][1].to_f
        #assert_equal tests[i][:full_message], emits[i][2]['full_message']
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
           '_custom_field' => 12345
         },
         :expected =>
         {
           'short_message' => 'short message',
           'full_message' => 'full message',
           'custom_field' => 12345
         }
        }
      ]

     driver.run(expect_emits: 1) do
        notifier = GELF::Notifier.new(k, PORT)

        tests.each { |test|
          notifier.notify!(test[:given])
        }
      end

      emits =driver.events
      assert_equal tests.length, emits.length, 'missing emitted events'
      emits.each_index { |i|
        assert_equal 'gelf', emits[i][0]
        assert_equal tests[i][:timestamp].to_f, emits[i][1] unless tests[i][:timestamp].nil?
        assert_block "expectation not met: #{tests[i][:expected]}" do
          emits[i][2].merge(tests[i][:expected]) == emits[i][2]
        end
      }
    }
  end

end
