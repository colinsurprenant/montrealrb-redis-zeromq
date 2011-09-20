require 'rubygems'
require 'raad'
require 'ffi-rzmq'

class ZeromqPubsubPublisher
  
  ZMQ_QUEUE_SIZE = 10
  ZMQ_LINGER_MS = 1000

  def start
    zmq_ctx = zmq_skt = nil
    zmq_ctx = ZMQ::Context.new(1)
    zmq_skt = zmq_ctx.socket(ZMQ::PUB)
    # zmq_skt.setsockopt(ZMQ::HWM, ZMQ_QUEUE_SIZE) # send queue high water mark 
    zmq_skt.setsockopt(ZMQ::LINGER, ZMQ_LINGER_MS) # how long to linger in mem for sending when calling shutdown
    zmq_skt_options = 'tcp://127.0.0.1:2211'
    zmq_skt.bind(zmq_skt_options)

    topic = "test_topic"

    Raad::Logger.info("starting ZeromqPubsubPublisher using #{zmq_skt_options.inspect}")
    i = 0
    while !Raad.stopped? 
      zmq_assert(zmq_skt.send_string(topic, ZMQ::SNDMORE))   # source topic header
      zmq_assert(zmq_skt.send_string((i += 1).to_s, ZMQ::NOBLOCK))  # data
      Raad::Logger.info("ZeromqPubsubPublisher sent=#{i}")
      sleep(1)
    end

  ensure
    zmq_skt.close if zmq_skt
    zmq_ctx.terminate if zmq_ctx
  end

  private

  def zmq_assert(code)
    raise("ZeromqPubsubPublisher zeromq invalid code=#{code.inspect}") unless code
  end


end

