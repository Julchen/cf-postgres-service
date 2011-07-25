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
      @app_id = "myapp"
      @opts = get_node_config()
      @logger = @opts[:logger]

      @node = Node.new(@opts)
      @resp = @node.provision("free")
      sleep 1

      @bind_resp = @node.bind(@resp['name'], 'rw')
      sleep 1

      EM.stop
    end
  end

  it "should have valid response" do
    @bind_resp.should_not be_nil
    @bind_resp['hostname'].should_not be_nil
    @bind_resp['port'].should_not be_nil
    @bind_resp['username'].should_not be_nil
    @bind_resp['password'].should_not be_nil
  end

  it "should be able to connect to postgresql" do
    is_port_open?('127.0.0.1', @resp['port']).should be_true
  end

  it "should allow authorized user to access the instance" do
    EM.run do
      conn = PGconn.connect('localhost', @resp['port'].to_i, '', '', @resp['db'], @bind_resp['username'], @bind_resp['password'])
      stat = conn.status
      stat.should == PGconn.CONNECTION_OK
      conn.exec("CREATE TABLE postgresql_unit_test (
                      name   varchar(40) 
                );")
      conn.exec("INSERT INTO postgresql_unit_test VALUES ('aa');")
      res = conn.query("SELECT * FROM postgresql_unit_test")
      
      res.ntuples.should == 1
      EM.stop
    end
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
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should not allow valid user with empty password to access the instance" do
    EM.run do
      begin
        conn = PGconn.connect('localhost', @resp['port'].to_i, '', '', @resp['db'], @bind_resp['username'], nil)
        conn.exec("CREATE TABLE postgresql_unit_test (
                      name   varchar(40) 
                );")
        conn.exec("INSERT INTO postgresql_unit_test VALUES ('aa');")
        res = conn.query("SELECT * FROM postgresql_unit_test")
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  # unbind here
  it "should be able to unbind it" do
    EM.run do
      resp  = @node.unbind(@bind_resp)
      resp.should be_true
      sleep 1
      EM.stop
    end
  end

  it "should not allow user to access the instance after unbind" do
    EM.run do
      begin
        conn = PGconn.connect('localhost', @resp['port'].to_i, '', '', @resp['db'], @bind_resp['login'], @bind_resp['secret'])
        conn.exec("CREATE TABLE postgresql_unit_test (
                      name   varchar(40) 
                );")
        conn.exec("INSERT INTO postgresql_unit_test VALUES ('aa');")
        res = conn.query("SELECT * FROM postgresql_unit_test")
      rescue => e
        e.should_not be_nil
      end
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

end
