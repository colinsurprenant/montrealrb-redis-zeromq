require 'rubygems'
require 'raad'
require 'redis'
require 'thread'

class RedisQueueSubscriber3

  def initialize
    @lock = Mutex.new
    @start_thread = nil
  end

  def start
    redis_options = {:host => 'localhost', :port => 6379}
    queue = 'test_queue'
    redis = Redis.new(redis_options)

    th = Thread.new do
      Thread.current.abort_on_exception = true
      Raad::Logger.info("starting RedisQueueSubscriber using #{redis_options.inspect}")
      loop do
        result = redis.blpop(queue, timeout = 0)
        Raad::Logger.info("RedisQueueSubscriber received=#{result.inspect}") if result
      end
    end

    @lock.synchronize{@start_thread = Thread.current}
    Thread.stop
    th.kill
  end

  def stop
    @lock.synchronize{@start_thread}.run
  end

end
