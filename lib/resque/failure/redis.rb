require 'time'
require 'resque/failure/each'

class Resque
  module Failure
    # A Failure backend that stores exceptions in Redis. Very simple but
    # works out of the box, along with support in the Resque web app.
    class Redis < Base
      # @overload (see Resque::Failure::Base#save)
      # @param (see Resque::Failure::Base#save)
      # @raise (see Resque::Failure::Base#save)
      # @return (see Resque::Failure::Base#save)
      def save
        data = {
          :failed_at => Time.now.rfc2822,
          :payload   => payload,
          :exception => exception.class.to_s,
          :error     => exception.to_s,
          :backtrace => filter_backtrace(Array(exception.backtrace)),
          :worker    => worker.to_s,
          :queue     => queue
        }
        data = client.encode(data)

        client.backend.store.rpush(:failed, data)
      end

      # @overload (see Resque::Failure::Base::count)
      # @param (see Resque::Failure::Base::count)
      # @raise (see Resque::Failure::Base::count)
      # @return (see Resque::Failure::Base::count)
      def self.count(client, queue = nil, class_name = nil)
        check_queue(queue)

        if class_name
          n = 0
          each(client, 0, count(client, queue), queue, class_name) { n += 1 }
          n
        else
          client.backend.store.llen(:failed).to_i
        end
      end

      # @overload (see Resque::Failure::Base::count)
      # @param (see Resque::Failure::Base::count)
      # @raise (see Resque::Failure::Base::count)
      # @return (see Resque::Failure::Base::count)
      def self.queues
        [:failed]
      end

      # @overload (see Resque::Failure::Base::all)
      # @param (see Resque::Failure::Base::all)
      # @return (see Resque::Failure::Base::all)
      def self.all(client, offset = 0, limit = 1, queue = nil)
        check_queue(queue)
        [client.list_range(:failed, offset, limit)].flatten
      end

      extend Each

      # @overload (see Resque::Failure::Base::clear)
      # @param (see Resque::Failure::Base::clear)
      # @return (see Resque::Failure::Base::clear)
      def self.clear(client, queue = nil)
        check_queue(queue)
        client.backend.store.del(:failed)
      end

      # @overload (see Resque::Failure::Base::requeue)
      # @param (see Resque::Failure::Base::requeue)
      # @return (see Resque::Failure::Base::requeue)
      def self.requeue(client, id)
        item = all(client, id).first
        item['retried_at'] = Time.now.rfc2822
        client.backend.store.lset(:failed, id, client.encode(item))
        Job.create(client, item['queue'], item['payload']['class'], *item['payload']['args'])
      end

      # @param id [Integer] index of item to requeue
      # @param queue_name [#to_s]
      # @return [void]
      def self.requeue_to(client, id, queue_name)
        item = all(client, id).first
        item['retried_at'] = Time.now.rfc2822
        client.backend.store.lset(:failed, id, client.encode(item))
        Job.create(client, queue_name, item['payload']['class'], *item['payload']['args'])
      end

      # @overload (see Resque::Failure::Base::remove)
      # @param (see Resque::Failure::Base::remove)
      # @return (see Resque::Failure::Base::remove)
      def self.remove(client, id)
        sentinel = ""
        client.backend.store.lset(:failed, id, sentinel)
        client.backend.store.lrem(:failed, 1,  sentinel)
      end

      # Requeue all items from failed queue where their original queue was
      # the given string
      # @param queue [String]
      # @return [void]
      def self.requeue_queue(client, queue)
        i = 0
        while job = all(client, i).first
           requeue(client, i) if job['queue'] == queue
           i += 1
        end
      end

      # Remove all items from failed queue where their original queue was
      # the given string
      # @param queue [String]
      # @return [void]
      def self.remove_queue(client, queue)
        i = 0
        while job = all(client, i).first
          if job['queue'] == queue
            # This will remove the failure from the array so do not increment the index.
            remove(client, i)
          else
            i += 1
          end
        end
      end

      # Ensures that the given queue is either nil or its to_s returns 'failed'
      # @param queue [nil,String]
      # @raise [ArgumentError] unless queue is nil or 'failed'
      # @return [void]
      def self.check_queue(queue)
        raise ArgumentError, "invalid queue: #{queue}" if queue && queue.to_s != "failed"
      end

      # Filter a backtrace, stripping everything above 'lib/resque/job.rb'
      # @param backtrace [Array<String>]
      # @return [Array<String>]
      def filter_backtrace(backtrace)
        backtrace.take_while { |item| !item.include?('/lib/resque/job.rb') }
      end
    end
  end
end
