require 'logstash/namespace'
require 'logstash/outputs/base'
require 'nsq'

class LogStash::Outputs::Nsq < LogStash::Outputs::Base
  config_name 'nsq'

  default :codec, 'json'
  config :nsqd, :validate => :string, :default => nil
  config :nsqlookupd, :validate => :array, :default => nil
  config :topic, :validate => :string, :required => true

  public
  def register
    options = {
        :nsqlookupd => @nsqlookupd,
        :topic => @topic,
    }
    if nsqlookupd == []
      options = {
          :nsqd => @nsqd,
          :topic => @topic,
      }
    end # if
    @producer = Nsq::Producer.new(options)
    #@producer.connect
    @logger.info('Registering nsq producer', :nsqd => @nsqd, :nsqlookupd => @nsqlookupd, :topic => @topic)

    @codec.on_event do |event, data|
      begin
        @producer.write(event.sprintf(data))
      rescue LogStash::ShutdownSignal
        @logger.info('nsq producer got shutdown signal')
      rescue => e
        @logger.warn('nsq producer threw exception, restarting',
                     :exception => e)
      end # begin
    end #do
  end # def register

  def receive(event)
    return unless output?(event)
    if event == LogStash::SHUTDOWN
      finished
      return
    end
    @codec.encode(event)
  end # def receive

  def teardown
    @producer.terminate
  end
end #class LogStash::Outputs::Nsq
