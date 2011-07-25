# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "pp"
#require "set"

require "datamapper"
require "nats/client"
require "uuidtools"
require "pg"

require 'vcap/common'
require 'vcap/component'

$:.unshift(File.dirname(__FILE__))

require "postgresql_service/util"
require "postgresql_service/storage_quota"

module VCAP; module Services; module Postgresql; end; end; end

class VCAP::Services::Postgresql::Node

  KEEP_ALIVE_INTERVAL = 15
  LONG_QUERY_INTERVAL = 1
  STORAGE_QUOTA_INTERVAL = 1

  include VCAP::Services::Postgresql::Util

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :user,       String,   :required => true
    property :password,   String,   :required => true
    property :plan,       Enum[:free], :required => true
    property :quota_exceeded,  Boolean, :default => false
  end

  def initialize(options)
    @logger = options[:logger]
    @logger.info("Starting Postgresql-Service-Node..")

    @local_ip = VCAP.local_ip(options[:ip_route])

    @node_id = options[:node_id]
    @postgres_config = options[:postgres]

    @max_db_size = options[:max_db_size] * 1024 * 1024
    @max_long_query = options[:max_long_query]

    @connection = postgres_connect

    EM.add_periodic_timer(KEEP_ALIVE_INTERVAL) {postgres_keep_alive}
    EM.add_periodic_timer(STORAGE_QUOTA_INTERVAL) {enforce_storage_quota}

    set_statment_timeout()

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir) if @base_dir

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    check_db_consistency()

    @available_storage = options[:available_storage] * 1024 * 1024
    ProvisionedService.all.each do |provisioned_service|
      @available_storage -= storage_for_service(provisioned_service)
    end

    @pids = {}

    @nats = NATS.connect(:uri => options[:mbus]) {on_connect}

    VCAP::Component.register(:nats => @nats,
                            :type => 'Postgresql-Service-Node',
                            :host => @local_ip,
                            :config => options)

  end

  def check_db_consistency()
    db_list = []
    @connection.exec('select d.datname as db, u.usename as user from pg_database d inner join pg_shadow u on d.datdba = u.usesysid ').each{|rslt| db_list.push([rslt['db'], rslt['user']])}    
    ProvisionedService.all.each do |service|
      db, user = service.name, service.user
      if not db_list.include?([db, user]) then
        @logger.info("Node database inconsistent!!! db:user <#{db}:#{user}> not in postgres.")
        next
      end
    end
  end

  def storage_for_service(provisioned_service)
    case provisioned_service.plan
    when :free then @max_db_size
    else
      raise "Invalid plan: #{provisioned_service.plan}"
    end
  end

  def postgres_connect
    host, user, password, port, socket =  %w{host user pass port socket}.map { |opt| @postgres_config[opt] }

    5.times do
      begin
        return PGconn.connect(host, port.to_i, '', '', 'postgres', user, password)
      rescue PGError => e
        @logger.info("Postgresql connection attempt failed: #{e}")
        sleep(5)
      end
    end

    @logger.fatal("Postgresql connection unrecoverable")
    shutdown
    exit
  end

  #keep connection alive, and check db liveness
  def postgres_keep_alive
    stat = @connection.status()
  rescue PGError => e
    @logger.info("Postgresql connection lost: #{e}")
    @connection = postgres_connect
  if (stat = PGconn.CONNECTION_BAD) then
    @logger.info("Postgresql connection lost. Reconnect.")
    @connection = postgres_connect
  end
  end

  #kill long queries
  def set_statment_timeout
    res = @connection.exec("SET statement_timeout TO '#{@max_long_query}min'") 
    rescue PGError => e
      @logger.info("Postgresql error: #{e}")
  end 

  def shutdown
    @logger.info("Shutting down..")
    @nats.close
  end

  def on_connect
    @logger.debug("Connected to mbus..")
    @nats.subscribe("PgaaS.provision.#{@node_id}") {|msg, reply| on_provision(msg, reply)}
    @nats.subscribe("PgaaS.unprovision.#{@node_id}") {|msg, reply| on_unprovision(msg, reply)}
    @nats.subscribe("PgaaS.unprovision") {|msg, reply| on_unprovision(msg, reply)}
    @nats.subscribe("PgaaS.discover") {|_, reply| send_node_announcement(reply)}
    send_node_announcement
    EM.add_periodic_timer(30) {send_node_announcement}
  end

  def on_provision(msg, reply)
    @logger.debug("Provision request: #{msg} from #{reply}")
    provision_message = Yajl::Parser.parse(msg)

    provisioned_service = ProvisionedService.new
    provisioned_service.name = "d-#{UUIDTools::UUID.random_create.to_s}".gsub(/-/, '')
    provisioned_service.user = 'u' + generate_credential
    provisioned_service.password = 'p' + generate_credential
    provisioned_service.plan = provision_message["plan"]

    create_database(provisioned_service)

    if not provisioned_service.save then
      delete_database(provisioned_service)
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    response = {
      "node_id" => @node_id,
      "hostname" => @local_ip,
      "port" => @postgres_config['port'],
      "password" => provisioned_service.password,
      "name" => provisioned_service.name,
      "user" => provisioned_service.user
    }
    @nats.publish(reply, Yajl::Encoder.encode(response))
    @logger.debug("Successfully provisioned database for request #{msg}: #{response.inspect}")
  rescue => e
    @logger.warn(e)
  end

  def on_unprovision(msg, reply)
    @logger.debug("Unprovision request: #{msg}.")
    unprovision_message = Yajl::Parser.parse(msg)

    provisioned_service = ProvisionedService.get(unprovision_message["name"])
    raise "Could not find service: #{unprovision_message["name"]}" if provisioned_service.nil?

    delete_database(provisioned_service)

    storage = storage_for_service(provisioned_service)
    @available_storage += storage

    raise "Could not delete service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy
    @logger.debug("Successfully fulfilled unprovision request: #{msg}.")
  rescue => e
    @logger.warn(e)
  end

  def send_node_announcement(reply = nil)
    @logger.debug("Sending announcement for #{reply || "everyone"}")
    response = {
      :id => @node_id,
      :available_storage => @available_storage
    }
    @nats.publish(reply || "PgaaS.announce", Yajl::Encoder.encode(response))
  end

  def create_database(provisioned_service)
    name, password, user = [:name, :password, :user].map { |field| provisioned_service.send(field) }
    begin
      start = Time.now
      @logger.debug("Creating: #{provisioned_service.pretty_inspect}")
      @logger.info("Creating credentials: #{user}/#{password}")
      @connection.query("CREATE USER #{user} WITH PASSWORD '#{password}'")
      @connection.query("CREATE DATABASE #{name} OWNER #{user}")
      @connection.query("GRANT ALL PRIVILEGES ON database #{name} to #{user}")
      @connection.query("REVOKE ALL PRIVILEGES ON database #{name} FROM PUBLIC")
      storage = storage_for_service(provisioned_service)
      @available_storage -= storage
      @logger.debug("Done creating #{provisioned_service.pretty_inspect}. Took #{Time.now - start}.")
    rescue => e
      @logger.warn("Could not create database: [] #{e.error}")
    end
  end

  def delete_database(provisioned_service)
    name, user = [:name, :user].map { |field| provisioned_service.send(field) }
    begin
      @logger.info("Deleting database: #{name}")
      @connection.exec("SELECT pg_terminate_backend(procpid) 
                       FROM pg_stat_activity 
                       WHERE datname = '#{name}' AND 
                             procpid <> pg_backend_pid()");   
      @connection.query("DROP DATABASE #{name}")
      @connection.query("DROP USER #{user}")      
    rescue PGError => e
      @logger.fatal("Could not delete database: #{e}")
    end
  end

end
