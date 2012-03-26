require 'em_test_helper'
require 'em/worker_queue'

class TestWorkerQueue < Test::Unit::TestCase
  def test_worker_as_queue
    EM.run {
      result = []
      add_result = proc{|value, task|
        result << value; task.done
      }
      check_result = proc {
          assert_equal [1,2,3], result
          EM.stop
        }
      worker = EM::WorkerQueue.new(add_result, check_result)
      worker.push 1
      worker.push 2
      worker.push 3
      worker.close
    }
  end

  class DetectConcurency
    attr_reader :max
    def initialize
      @max, @workers = 0, 0
    end

    def for_each(value, task)
      @workers += 1
      @max = @workers  if @workers > @max
      EM.add_timer(0.001) do
        @workers -= 1
        task.done
      end
    end
  end

  def detecter
    @detecter ||= DetectConcurency.new
  end

  def test_concurency_level_1
    EM.run {
      worker = EM::WorkerQueue.new(
        detecter.method(:for_each),
        proc{
          assert_equal 1, detecter.max
          EM.stop
        }
      )
      100.times{|i| worker.push(i)}
      worker.close
    }
  end

  def test_concurrency_level_more
    EM.run {
      worker = EM::WorkerQueue.new(
        detecter.method(:for_each),
        proc{
          assert_equal 4, detecter.max
          EM.stop
        },
        :concurrency => 4
      )
      100.times{|i| worker.push(i)}
      worker.close
    }
  end

  def test_change_concurrency
    EM.run {
      worker = EM::WorkerQueue.new(
        detecter.method(:for_each),
        proc{
          assert_equal 4, detecter.max
          EM.stop
        }
      )
      100.times{|i| worker.push(i)}
      EM.add_timer(0.005) {
        worker.concurrency = 4
      }
      worker.close
    }
  end
end
