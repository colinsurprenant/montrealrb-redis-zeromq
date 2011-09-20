require 'rubygems'
require 'raad'
require 'redis'

class RedisQueuePublisher

  def initialize
    @redis_options = {:host => 'localhost', :port => 6379}
    @queue = 'test_queue'
    @redis = Redis.new(@redis_options)
  end

  def start
    Raad::Logger.info("starting RedisQueuePublisher using #{@redis_options.inspect}")
    i = 0
    while !Raad.stopped? 
      result = @redis.rpush(@queue, i += 1)
      Raad::Logger.info("RedisQueuePublisher sent=#{i}")
      sleep(1)
    end
  end

end

