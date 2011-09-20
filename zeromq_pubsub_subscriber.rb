require 'rubygems'
require 'raad'
require 'ffi-rzmq'

class ZeromqPubsubSubscriber
  
  def start
    zmq_ctx = zmq_skt = nil
    zmq_ctx = ZMQ::Context.new(1)
    zmq_skt = zmq_ctx.socket(ZMQ::SUB)
    zmq_skt.setsockopt(ZMQ::SUBSCRIBE, '') # '' matches all topics, otherwise match on topics beginning with specified prefix 
    zmq_skt_options = 'tcp://127.0.0.1:2211'
    zmq_skt.connect(zmq_skt_options)

    topic = "test_topic"

    Raad::Logger.info("starting ZeromqPubsubSubscriber using #{zmq_skt_options.inspect}")

    while !Raad.stopped? 
      topic = zmq_skt.recv_string
      item = zmq_skt.more_parts? ? zmq_skt.recv_string : nil
    
      if item
        Raad::Logger.error("ZeromqPubsubSubscriber received=#{topic}/#{item.inspect}")
      else
        Raad::Logger.error("ZeromqPubsubSubscriber unable to receice all parts for topic=#{topic}")
      end
    end

  ensure
    zmq_skt.close if zmq_skt
    zmq_ctx.terminate if zmq_ctx
  end

  private

  def zmq_assert(code)
    raise("ZeromqPubsubSubscriber zeromq invalid code=#{code.inspect}") unless code
  end


end

