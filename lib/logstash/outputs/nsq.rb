require 'logstash/namespace'
require 'logstash/outputs/base'
require 'nsq'

class LogStash::Outputs::Nsq < LogStash::Outputs::Base
    config_name 'nsq'

    default :codec, 'json'

    config :nsqd, :validate => :string, :default => nil
    config :nsqlookupd, :validate => :array, :default => nil
    config :topic, :validate => :string, :required => true
    config :tls_v1, :validate => :boolean, :default => false
    config :tls_key, :validate => :string
    config :tls_cert, :validate => :string
 
  public
  def register
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
  end # def register

  def write_to_nsq(payload)
    @producer.write(payload)
  end # def write_to_nsq

  def multi_receive_encoded(encoded)
    encoded.each do |event,data|
      write_to_nsq(data)
    end
  end

  def close
    @logger.info('closing nsq producer')
    @producer.terminate
  end

  private

end #class LogStash::Outputs::Nsq

