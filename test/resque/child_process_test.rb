require 'test_helper'

require 'resque/child_process'

describe Resque::ChildProcess do
  let(:client) { MiniTest::Mock.new }
  let(:resque) { Resque.new }

  describe "#reconnect" do
    it "delegates to the client" do
      client.expect :reconnect, nil
      worker = Resque::Worker.new resque, :foo, :client => client
      child_process = Resque::ChildProcess.new(resque, worker)
      child_process.reconnect
    end
  end
end
