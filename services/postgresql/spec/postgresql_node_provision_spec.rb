# Copyright (c) 2009-2011 VMware, Inc.
require "spec_helper"
require "postgresql_service/node"
require "pg"

include VCAP::Services::Postgresql


module VCAP
  module Services
    module Postgresql
      class Node
        attr_reader :available_memory
      end
    end
  end
end

describe VCAP::Services::Postgresql::Node do

  before :all do
    EM.run do
      @opts = get_node_config()
      @logger = @opts[:logger]
      @node = Node.new(@opts)
      @original_memory = @node.available_memory

      @resp = @node.provision("free")
      sleep 1
      EM.stop
    end
  end

  it "should have valid response" do
    @resp.should_not be_nil
    puts @resp
    inst_name = @resp['name']
    inst_name.should_not be_nil
    inst_name.should_not == ""
  end

  it "should consume node's memory" do
    (@original_memory - @node.available_memory).should > 0
  end

  it "should be able to connect to postgresql" do
    is_port_open?('127.0.0.1', @resp['port']).should be_true
  end

  it "should not allow unauthorized user to access the instance" do
    EM.run do
      begin
        conn = PGconn.connect('localhost', @resp['port'].to_i, '', '', @resp['db'], nil, nil)
        conn.exec("CREATE TABLE postgresql_unit_test (
                      name   varchar(40) 
                );")
        conn.exec("INSERT INTO postgresql_unit_test VALUES ('aa');")
        res = conn.query("SELECT * FROM postgresql_unit_test")      
      rescue Exception => e
        @logger.debug e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  # unprovision here
  it "should be able to unprovision an existing instance" do
    EM.run do
      @node.unprovision(@resp['name'], [])

      e = nil
      begin
        conn = PGconn.connect('localhost', @resp['port'].to_i, '', '', @resp['db'], nil, nil)
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should release memory" do
    EM.run do
      @original_memory.should == @node.available_memory
      EM.stop
    end
  end

end
