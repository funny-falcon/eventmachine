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
    # @overload new(on_done, &foreach)
    # @overload new(foreach, on_done, concurrency_level = 1)
    # @overload new(opts, &foreach)
    # @overload new(foreach, opts)
    #
    # possible options:
    #   :foreach     - proc called on every element
    #   :on_done     - proc called after queue is closed
    #   :concurrency - concurency level
    #   :on_empty    - proc called when there is available level of concurency
    def initialize(*args, &cb)
      opts = Hash === args.last ? args.pop : {}
      @concurrency = opts[:concurrency]
      @concurrency ||= ::Numeric === args[2] ? args[2] : 1
      if args[1].respond_to? :call
        raise ArgumentError, "Should not provide both proc and block"  if cb
        @foreach = args[0]
        @on_done = args[1]
      else
        @foreach = args[0] || cb || opts[:foreach]
        raise ArgumentError, "Should provide proc or block callback"  unless @foreach
        @on_done = opts[:on_done]
      end
      @items = []
      @pending = 0
      @closed = false
      @finished = false
      on_empty opts[:on_empty]
    end

    # Set concurrency level, spawn more workers if there are waiting items
    def concurrency= v
      @concurrency, more = v, @concurrency < v
      EM.schedule self  if more
      v
    end

    # Push a value to be served by worker
    def push(value)
      unless EM.reactor_running? && EM.reactor_thread?
        raise NotInReactor, "You should call WorkerQueue#push inside EM reactor (use EM.schedule)"
      end
      raise QueueClosed, "You not allowed to push into a closed WorkerQueue"  if @closed
      @on_empty_cnt -= 1  if @on_empty_cnt > 0
      if @pending < @concurrency
        @pending += 1
        EM.next_tick spawn_worker(value)
      else
        @items << value
      end
    end
    alias :<< :push

    # Setup pull callback which will pull new values for workers
    # Callback should call WorkerQueue#push if there is new values
    # or WorkerQueue#close if iteration is over
    #
    # If you use #on_empty callback, you should explicitely call #run
    #
    # @example
    #
    # jobs = Queue.new
    # sum = 0
    # wq = WorkerQueue.new(
    #     proc{|v, w| sum += v; w.done},
    #     proc{ puts sum },
    #     :concurency => 10)
    # wq.on_empty {|_wq| jobs.pop{|v| _wq.push(v)} }
    #
    # jobs.push(1)
    # jobs.push(2)
    # jobs.push(3)
    def on_empty(*args, &blk)
      @on_empty_cnt = 0
      unless args.empty? && blk.nil?
        @on_empty = EM.Callback(*args, &blk)
      else
        @on_empty = nil
      end
      EM.next_tick self
    end

    # Stop waiting for a new values
    def close
      EM.schedule {
        @closed = true
        call
      }
    end
    alias closed?  closed
    alias stop     close
    alias stopped? closed?

    def _return_worker
      EM.schedule do
        @pending -= 1
        if !check_items
          EM.next_tick self
        end
      end
    end

    def call
      if check_items
        EM.next_tick self
      elsif @closed && @pending == 0
        if @on_done && !@finished
          EM.next_tick @on_done
        end
        @finished = true
      end
    end

    def run
      @closed = false
      @finished = false
      call
    end

    private

    def check_items
      if @pending < @concurrency
        if !@items.empty?
          @pending += 1
          EM.next_tick spawn_worker(@items.shift)
          true
        elsif @on_empty && !@closed && @on_empty_cnt < @concurrency
          @on_empty_cnt += 1
          if @on_empty_cnt <= @concurrency
            @on_empty.arity == 0 ? @on_empty.call : @on_empty.call(self)
          end
          true
        end
      end
    end

    def spawn_worker(value)
      self.class::Worker.new(self, value, @foreach)
    end
  end
end
