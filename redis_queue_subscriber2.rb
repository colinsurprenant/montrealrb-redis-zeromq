require 'rubygems'
require 'raad'
require 'redis'

class RedisQueueSubscriber2

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

    sleep(1) while !Raad.stopped?
    th.kill
  end

end

