module EventMachine
  # A parallel worker queue
  #
  # This class provides queue of tasks served by one or more identical workers
  #
  # It is sweeter than using EM::Queue for same purpose
  # and doesn't try to do as much as EM::Iterator
  #
  # @example
  #
  #  wq = EM::WorkerQueue.new(:concurrency => 4){|v, task|
  #     puts(v); task.done
  #  }
  #  100.times{|i| wq.push i }
  #
  #  sum = 0
  #  foreach = proc{|v, t| sum += v; t.done}
  #  on_done = proc{ puts "Sum: #{sum}" }
  #  wq = EM::WorkerQueue.new(foreach, on_done, :concurrency => 10)
  #
  #  100.times{|i| wq.push i}
  #  wq.close
  #
  class WorkerQueue
    class QueueClosed < StandardError; end
    class AlreadyDone < StandardError; end
    class NotInReactor < StandardError; end
    class Worker
      attr_accessor :value
      def initialize(master, value, callback)
        @master   = master
        @value    = value
        @callback = callback
      end
      def done?
        @master.nil?
      end
      def done
        raise AlreadyDone, "Task already done"  if done?
        @master._return_worker
        @value = @master = nil
      end
      def call
        @callback.call(@value, self)
      end
    end

    attr_reader :concurrency, :closed

    # Create a new worker queue with callbacks and concurrency level
    #
    # @overload new(&foreach)
    # @overload new(foreach, on_done, concurrency_level = 1)
    # @overload new(:on_done => on_done, :concurrency_level => 1, &foreach)
    # @overload new(foreach, :on_done => on_done, :concurrency_level => 1)
    def initialize(*args, &cb)
      opts = Hash === args.last ? args.pop : {}
      @concurrency = opts[:concurrency]
      @concurrency ||= ::Numeric === args[2] ? args[2] : 1
      @foreach = args[0] || cb
      @on_done = args[1] || opts[:on_done]
      @items = []
      @pending = 0
      @closed = false
    end

    # Set concurrency level, spawn more workers if there are waiting items
    def concurrency= v
      @concurrency = v
      EM.schedule self
      v
    end

    # Push a value to be served by worker
    def push(value)
      unless EM.reactor_running? && EM.reactor_thread?
        raise NotInReactor, "You should call WorkerQueue#push inside EM reactor (use EM.schedule)"
      end
      raise QueueClosed, "You not allowed to push into a closed WorkerQueue"  if @closed
      if @pending < @concurrency
        @pending += 1
        EM.next_tick spawn_worker(value)
      else
        @items << value
      end
    end
    alias :<< :push

    # Stop waiting for a new values
    def close
      EM.schedule {
        @closed = true
        call
      }
    end
    alias :closed? :closed

    def _return_worker
      EM.schedule do
        @pending -= 1
        if !@items.empty?
          if @pending < @concurrency
            @pending += 1
            EM.next_tick spawn_worker(@items.shift)
          end
        else
          EM.next_tick self
        end
      end
    end

    def call
      if !@items.empty?
        if @pending < @concurrency
          @pending += 1
          EM.next_tick spawn_worker(@items.shift)
          EM.next_tick self  if @pending < @concurrency
        end
      elsif @closed && @pending == 0 && @on_done
        EM.next_tick @on_done
      end
    end

    private

    def spawn_worker(value)
      self.class::Worker.new(self, value, @foreach)
    end
  end
end
