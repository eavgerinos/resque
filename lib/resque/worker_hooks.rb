class Resque
  # Runs Resque hooks
  class WorkerHooks
    attr_reader :logger, :client

    def initialize(client, logger)
      @client = client
      @logger = logger
    end

    # Runs a named hook, passing along any arguments.
    def run_hook(name, *args)
      return unless hooks = client.send(name)
      msg = "Running #{name} hooks"
      msg << " with #{args.inspect}" if args.any?
      logger.info msg

      hooks.each do |hook|
        args.any? ? hook.call(*args) : hook.call
      end
    end
  end
end
