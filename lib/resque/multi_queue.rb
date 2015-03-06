require 'redis'
require 'redis-namespace'
require 'thread'
require 'mutex_m'

class Resque
  ###
  # Holds multiple queues, allowing you to pop the first available job
  class MultiQueue
    include Mutex_m

    attr_reader :client

    ###
    # Create a new MultiQueue using the +queues+ from the +redis+ connection
    # @param queues [Array<Resque::Queue>]
    # @param client [Resque]
    def initialize(queues, client)
      super

      @queues     = queues
      @queue_hash = {}
      @client     = client

      queues.each do |queue|
        key = queue.redis_name
        @queue_hash[key] = queue
      end
    end

    # Factory method, given a list of queues, give us a
    # multiqueue
    # @param queues [Array<#to_s>]
    # @param client [Resque]
    # @return [Resque::MultiQueue]
    def self.from_queues(queues, client)
      new_queues = queues.map do |queue|
        Queue.new(queue, client)
      end

      new(new_queues, client)
    end

    # Pop an item off one of the queues.  This method will block until an item
    # is available. This method returns a tuple of the queue object and job.
    #
    # Pass +true+ for a non-blocking pop.  If nothing is read on a non-blocking
    # pop, a ThreadError is raised.
    # @param non_block [Boolean] (false)
    # @return [Array<Object>]
    #   a tuple whose first element is the queue [Resque::Queue]
    #   and whose second element is the decoded payload [Hash<String,Object>]
    def pop(non_block = false)
      if non_block
        synchronize do
          @queues.each do |queue|
            begin
              return [queue, queue.pop(true)]
            rescue ThreadError
            end end

          raise ThreadError
        end
      else
        queue_names = @queues.map {|queue| queue.redis_name }
        if queue_names.any?
          synchronize do
            value = @redis.blpop(*(queue_names + [1])) until value
            queue_name, payload = value
          queue = @queue_hash[queue_name]
            [queue, queue.decode(payload)]
          end
        else
          Kernel.sleep # forever
        end
      end
    end

    # Retrieves data from the queue head, and removes it.
    #
    # Blocks for +timeout+ seconds if the queue is empty, and returns nil if
    # the timeout expires.
    # @param timeout [Numeric]
    def poll(timeout)
      queue_names = @queues.map {|queue| queue.redis_name }
      if queue_names.any?
        queue_name, payload = client.redis.blpop(*(queue_names + [timeout.to_i]))
        return unless payload

        synchronize do
          queue = @queue_hash[queue_name]
          [queue, queue.decode(payload)]
        end
      else
        Kernel.sleep(timeout)
        nil
      end
    end
  end
end
