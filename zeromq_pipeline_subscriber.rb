require 'rubygems'
require 'raad'
require 'ffi-rzmq'

class ZeromqPipelineSubscriber
  
  def start
    zmq_ctx = zmq_skt = nil
    zmq_ctx = ZMQ::Context.new(1)
    zmq_skt = zmq_ctx.socket(ZMQ::PULL)
    zmq_skt_options = 'tcp://127.0.0.1:2210'
    zmq_skt.connect(zmq_skt_options)

    topic = "test_topic"

    Raad::Logger.info("starting ZeromqPipelineSubscriber using #{zmq_skt_options.inspect}")

    while !Raad.stopped? 
      topic = zmq_skt.recv_string
      item = zmq_skt.more_parts? ? zmq_skt.recv_string : nil
    
      if item
        Raad::Logger.error("ZeromqPipelineSubscriber received=#{topic}/#{item.inspect}")
      else
        Raad::Logger.error("ZeromqPipelineSubscriber unable to receice all parts for topic=#{topic}")
      end
    end

  ensure
    zmq_skt.close if zmq_skt
    zmq_ctx.terminate if zmq_ctx
  end

  private

  def zmq_assert(code)
    raise("ZeromqPipelineSubscriber zeromq invalid code=#{code.inspect}") unless code
  end


end

