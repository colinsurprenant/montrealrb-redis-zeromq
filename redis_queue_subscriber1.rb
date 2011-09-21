require 'rubygems'
require 'raad'
require 'redis'

class RedisQueueSubscriber1

  def initialize
    @redis_options = {:host => 'localhost', :port => 6379}
    @queue = 'test_queue'
    @redis = Redis.new(@redis_options)
  end

  def start
    Raad::Logger.info("starting RedisQueueSubscriber1 using #{@redis_options.inspect}")
    while !Raad.stopped?
      result = @redis.blpop(@queue, timeout = 0)
      Raad::Logger.debug("RedisQueueSubscriber1 received=#{result.inspect}") if result
    end
  end

end

