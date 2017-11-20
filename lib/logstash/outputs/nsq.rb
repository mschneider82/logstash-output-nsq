require 'logstash/namespace'
require 'logstash/outputs/base'
require 'java'

class LogStash::Outputs::Nsq < LogStash::Outputs::Base
    declare_threadsafe!
    config_name 'nsq'

    default :codec, 'json'

    default :codec, 'json'
    config :nsqd, :validate => :string, :default => nil
    config :nsqlookupd, :validate => :array, :default => nil
    config :topic, :validate => :string, :required => true
    config :tls_v1, :validate => :boolean, :default => false
    config :tls_key, :validate => :string
    config :tls_cert, :validate => :string
 
  public
  def register
    @thread_batch_map = Concurrent::Hash.new

    require 'nsq'
    options = {
        :nsqlookupd => @nsqlookupd,
        :topic => @topic,
        :tls_v1 => @tls_v1
    }
    # overwrite nsqlookupd options if client certificate validation is used:
    # this is very dirty. please fix 
    if @tls_key and @tls_cert
      options = {
          :nsqlookupd => @nsqlookupd,
          :topic => @topic,
          :tls_v1 => @tls_v1,
          :tls_context => {
           key: @tls_key,
           certificate: @tls_cert
          }
      }
    end
    # overwrite options if no nsqlookupd is used:
    if nsqlookupd == []
      if @tls_key and @tls_cert
        options = {
            :nsqd => @nsqd,
            :topic => @topic,
            :tls_v1 => @tls_v1,
            :tls_context => {
             key: @tls_key,
             certificate: @tls_cert
            }
        }
      else
        options = {
            :nsqd => @nsqd,
            :topic => @topic,
            :tls_v1 => @tls_v1
        }
      end
    end # if

    @producer = Nsq::Producer.new(options)
    @logger.info('Registering nsq producer', :nsqd => @nsqd, :nsqlookupd => @nsqlookupd, :topic => @topic)
    @codec.on_event do |event, data|
        write_to_nsq(event, data)
    end
  end # def register

  def write_to_nsq(event, data)
    begin
      @producer.write(event.sprintf(data))
    rescue LogStash::ShutdownSignal
      @logger.info('nsq producer got shutdown signal')
    rescue => e
      @logger.warn('nsq producer threw exception, restarting', :exception => e)
    end # begin
  end # def send_to_nsq

  def prepare(record)
    # This output is threadsafe, so we need to keep a batch per thread.
    @thread_batch_map[Thread.current].add(record)
  end

  def multi_receive(events)
    t = Thread.current
    if !@thread_batch_map.include?(t)
      @thread_batch_map[t] = java.util.ArrayList.new(events.size)
    end

    events.each do |event|
      break if event == LogStash::SHUTDOWN
      @codec.encode(event)
    end

    batch = @thread_batch_map[t]
    if batch.any?
      retrying_send(batch)
      batch.clear
    end
  end

  def close
    @logger.info('closing nsq producer')
    @producer.terminate
  end

  private

end #class LogStash::Outputs::Nsq

