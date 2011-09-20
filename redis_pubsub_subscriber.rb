require 'rubygems'
require 'raad'
require 'redis'
require 'thread'

class RedisPubsubSubscriber

  def initialize
    @lock = Mutex.new
    @start_thread = nil
  end

  def start
    @lock.synchronize{@start_thread = Thread.current}

    redis_options = {:host => 'localhost', :port => 6379}
    channels = ['test_channel1', 'test_channel2']
    redis = Redis.new(redis_options)

    subscriber = Thread.new do
      Thread.current.abort_on_exception = true
      Raad::Logger.info("starting RedisPubSubSubscriber using #{redis_options.inspect}")

      redis.subscribe(*channels) do |on|
        on.subscribe do |channel, subscriptions|
          Raad::Logger.info("RedisPubSubSubscriber subscribed to #{channel} (#{subscriptions} subscriptions)")
        end

        on.message do |channel, message|
          Raad::Logger.info("RedisPubSubSubscriber received=#{channel}/#{message}")
          redis.unsubscribe(channel) if message == "END"
        end

        on.unsubscribe do |channel, subscriptions|
          Raad::Logger.info("RedisPubSubSubscriber unsubscribed from #{channel} (#{subscriptions} subscriptions)")
        end
      end

      Raad::Logger.info("RedisPubSubSubscriber subscribe loop ended")
      stop
    end

    Thread.stop
    subscriber.kill
  end

  def stop
    @lock.synchronize{@start_thread}.run
  end

end
