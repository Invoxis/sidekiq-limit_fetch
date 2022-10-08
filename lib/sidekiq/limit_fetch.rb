require 'forwardable'
require 'sidekiq'
require 'sidekiq/manager'
require 'sidekiq/api'

module Sidekiq::LimitFetch
  autoload :UnitOfWork, 'sidekiq/limit_fetch/unit_of_work'

  require_relative 'limit_fetch/instances'
  require_relative 'limit_fetch/queues'
  require_relative 'limit_fetch/global/semaphore'
  require_relative 'limit_fetch/global/selector'
  require_relative 'limit_fetch/global/monitor'
  require_relative 'extensions/queue'
  require_relative 'extensions/manager'

  TIMEOUT = Sidekiq::BasicFetch::TIMEOUT

  extend self

  CLEAN_INTERVAL          = 60 * 60  # 1 hour

  WATCHDOG_INTERVAL       = 20       # seconds
  WATCHDOG_LIFESPAN       = 60       # seconds
  WATCHDOG_RETRY_DELAY    = 1        # seconds

  WORKING                 = 'working'

  ATTEMPT_INTERVAL        = 2 * 60 # seconds

  ATTEMPT_CLEAN_LOCK_KEY  = 'reliable-limit-fetch-attempt-clean-lock'

  SCAN_NUMBER             = 1000

  MAX_RETRIES_AFTER_INTERRUPTION = 3


  def new(_)
    self
  end

  def retrieve_work
    clean_working_queues! if try_to_clean?

    queue, job = redis_brpop(Queues.acquire)
    Queues.release_except(queue)
    if job 
      work = UnitOfWork.new(queue, job)
      Sidekiq.redis do |conn|
        conn.lpush(Sidekiq::LimitFetch.working_queue_name(queue), job)
      end
      work
    end
  end

  def bulk_requeue(inprogress, options)
    # Sidekiq::BasicFetch.new(options).bulk_requeue(inprogress, options)
    # the codes below are copy pasted from the original sidekiq/fetch.rb 
    return if inprogress.empty?

    Sidekiq.logger.debug { "Re-queueing terminated jobs" }
    jobs_to_requeue = {}
    inprogress.each do |unit_of_work|
      jobs_to_requeue[unit_of_work.queue] ||= []
      jobs_to_requeue[unit_of_work.queue] << unit_of_work.job
    end

    Sidekiq.redis do |conn|
      conn.pipelined do |pipeline|
        jobs_to_requeue.each do |queue, jobs|
          pipeline.rpush(queue, jobs)

          pipeline.lrem(Sidekiq::LimitFetch.working_queue_name(queue), 1, jobs)
        end
      end
    end
    Sidekiq.logger.info("Pushed #{inprogress.size} jobs back to Redis")
  rescue => ex
    Sidekiq.logger.warn("Failed to requeue #{inprogress.size} jobs: #{ex.message}")
  end

  def config
    # Post 6.5, Sidekiq.options is deprecated and replaced with passing Sidekiq directly
    post_6_5? ? Sidekiq : Sidekiq.options
  end

  # Backwards compatibility for sidekiq v6.1.0
  # @see https://github.com/mperham/sidekiq/pull/4602
  # def bulk_requeue(*args)
  #   if Sidekiq::BasicFetch.respond_to?(:bulk_requeue) # < 6.1.0
  #     Sidekiq::BasicFetch.bulk_requeue(*args)
  #   else # 6.1.0+
  #     Sidekiq::BasicFetch.new(config).bulk_requeue(*args)
  #   end
  # end

  def redis_retryable
    yield
  rescue Redis::BaseConnectionError
    sleep TIMEOUT
    retry
  rescue Redis::CommandError => error
    # If Redis was restarted and is still loading its snapshot,
    # then we should treat this as a temporary connection error too.
    if error.message =~ /^LOADING/
      sleep TIMEOUT
      retry
    else
      raise
    end
  end

  class << self
    def working_queue_name(queue)
      "#{WORKING}:#{queue}:#{hostname}:#{pid}"
    end

    def pid
      @pid ||= ::Process.pid
    end

    def hostname
      @hostname ||= Socket.gethostname
    end
  end

  def self.worker_down?(hostname, pid, conn)
    !conn.get(watchdog_key(hostname, pid))
  end

  def clean_working_queues!
    Sidekiq.logger.info('Cleaning working queues')

    Sidekiq.redis do |conn|
      conn.scan_each(match: "#{WORKING}:queue:*", count: SCAN_NUMBER) do |key|

        hostname, pid = key.scan(/:([^:]*):([0-9]*)\z/).flatten

        continue if hostname.nil? || pid.nil?

        clean_working_queue!(key) if Sidekiq::LimitFetch.worker_down?(hostname, pid, conn)
      end
    end
  end

  def self.setup_reliable_limite_fetch!
      start_watchdog_thread
  end

  def self.start_watchdog_thread
    Thread.new do
      loop do                                                                                                     
        begin      
          watchdog_is_watching                         

          sleep WATCHDOG_INTERVAL
        rescue => e
          Sidekiq.logger.error("Watchdog thread error: #{e.message}")

          sleep WATCHDOG_RETRY_DELAY
        end
      end
    end
  end

  def self.watchdog_is_watching
    Sidekiq.redis do |conn|
      conn.set(watchdog_key(hostname, pid), 1, ex: WATCHDOG_LIFESPAN)
    end

    Sidekiq.logger.info("Watchdog is watching for hostname: #{hostname} and pid: #{pid}")
  end

  def self.watchdog_key(hostname, pid)
    "reliable-limit-fetch-watchdog-#{hostname}-#{pid}"
  end


  private

  def clean_working_queue!(working_queue)
    original_queue = working_queue.gsub(/#{WORKING}:|:[^:]*:[0-9]*\z/, '')

    Sidekiq.redis do |conn|
      while job = conn.rpop(working_queue)
        process_interrupted_job(job, original_queue)
      end
    end
  end

  def process_interrupted_job(job, queue)
    msg = Sidekiq.load_json(job)
    msg['interrupted_count'] = msg['interrupted_count'].to_i + 1

    if msg['interrupted_count'] >= MAX_RETRIES_AFTER_INTERRUPTION
      send_to_dead_set(queue, msg)
    else
      requeue_job(queue, msg)
    end
  end

  def send_to_dead_set(queue, msg)
    job = Sidekiq.dump_json(msg)
    Sidekiq::DeadSet.new.kill(job, notify_failure: false)

    Sidekiq.logger.info(
      message: "Cleaning working queues: Added dead class #{msg['class']} : queue #{queue} : job #{msg['jid']} to dead set",
      jid: msg['jid'],
      queue: queue,
      class: msg['class'],
    )
  end

  def requeue_job(queue, msg)
    Sidekiq.redis do |conn|
      conn.lpush(queue, Sidekiq.dump_json(msg))
    end

    Sidekiq.logger.info(
      message: "Cleaning working queues: Pushed class #{msg['class']} : job #{msg['jid']} back to queue #{queue}",
      jid: msg['jid'],
      queue: queue,
      class: msg['class'],
    )
  end

  def try_to_clean?
    return if still_not_cleanable?

    last_attempt = Time.now.to_f
    Sidekiq.redis do |conn|
      conn.set(ATTEMPT_CLEAN_LOCK_KEY, 1, nx: true, ex: CLEAN_INTERVAL)
    end
  end

  def last_attempt
    @last_attempt ||= 0
  end

  def still_not_cleanable?
    Time.now.to_f - last_attempt < ATTEMPT_INTERVAL
  end

  def post_6_5?
    @post_6_5 ||= Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new('6.5.0')
  end

  def redis_brpop(queues)
    if queues.empty?
      sleep TIMEOUT  # there are no queues to handle, so lets sleep
      []             # and return nothing
    else
      redis_retryable { Sidekiq.redis { |it| it.brpop *queues, timeout: TIMEOUT } }
    end
  end
end
