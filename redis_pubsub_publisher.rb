require 'rubygems'
require 'raad'
require 'redis'

class RedisPubsubPublisher

  def initialize
    @redis_options = {:host => 'localhost', :port => 6379}
    @channels = ['test_channel1', 'test_channel2']
    @redis = Redis.new(@redis_options)
  end

  def start
    Raad::Logger.info("starting RedisPubSubPublisher using #{@redis_options.inspect}")
    i = 0
    while !Raad.stopped? 
      i += 1
      @channels.each{|c| @redis.publish(c, i)}
      Raad::Logger.info("RedisPubSubPublisher sent=#{i}")
      sleep(1)
    end
  end

end

