require 'test_helper'
require 'resque/failure/redis'

describe Resque::Failure::Redis do
  before do
    @resque ||= Resque.new
    @resque.redis = "redis://localhost:6379"
  end

  after do
    @resque.backend.store.flushall
  end

  describe '#count' do
    it 'counts all failures' do
      save_failure
      save_failure
      save_failure

      assert_equal 3, Resque::Failure::Redis.count(@resque)
    end

    it 'counts all failures for the given queue and class' do
      save_failure(:failed, 'some_class')
      save_failure(:failed, 'another_class')
      save_failure(:failed, 'another_class')

      assert_equal 1, Resque::Failure::Redis.count(@resque, :failed, 'some_class')
      assert_equal 2, Resque::Failure::Redis.count(@resque, :failed, 'another_class')
    end
  end

  describe '#queues' do
    it 'returns the failure queue' do
      assert_equal [:failed], Resque::Failure::Redis.queues
    end
  end

  describe '#requeue' do
    it 'requeues a new job to the queue of the failed job' do
      save_failure

      failure = Resque::Failure::Redis.all(@resque).first
      assert_nil failure['retried_at']

      Resque::Failure::Redis.requeue(@resque, 0)

      job = Resque::Job.reserve(@resque, :failed)
      assert_equal 'some_class', job.payload['class']
      assert_equal ['some_args'], job.args

      failure = Resque::Failure::Redis.all(@resque).first
      refute_nil failure['retried_at']
    end
  end

  describe '#requeue_to' do
    it 'requeues a new job to the desired queue' do
      save_failure

      failure = Resque::Failure::Redis.all(@resque).first
      assert_nil failure['retried_at']

      Resque::Failure::Redis.requeue_to(@resque, 0, :new_queue)

      job = Resque::Job.reserve(@resque, :new_queue)
      assert_equal 'some_class', job.payload['class']
      assert_equal ['some_args'], job.args

      failure = Resque::Failure::Redis.all(@resque).first
      refute_nil failure['retried_at']
    end
  end

  describe '#requeue_queue' do
    it 'requeues all failures for the desired queue' do
      save_failure('queue1')
      save_failure('queue2')
      save_failure('queue1')
      save_failure('queue3')

      Resque::Failure::Redis.requeue_queue(@resque, 'queue1')

      2.times do
        job = Resque::Job.reserve(@resque, 'queue1')
        refute_nil job
        assert_equal 'queue1', job.queue
      end

      assert_nil Resque::Job.reserve(@resque, 'queue1')
    end
  end

  describe '#remove_queue' do
    it 'removes all failures for the desired queue' do
      save_failure('queue1')
      save_failure('queue2')
      save_failure('queue1')
      save_failure('queue3')

      Resque::Failure::Redis.remove_queue(@resque, 'queue1')

      assert_equal 2, Resque::Failure.count(@resque)
      assert_equal 'queue2', Resque::Failure::Redis.all(@resque, 0).first['queue']
      assert_equal 'queue3', Resque::Failure::Redis.all(@resque, 1).first['queue']
    end
  end

  private

  def save_failure(queue = :failed, klass = 'some_class', args = 'some_args')
    failure = Resque::Failure::Redis.new(Exception.new,
                                         @resque,
                                         nil, queue,
                                         {'class' => klass,
                                          'args' => args})
    failure.save
  end
end
