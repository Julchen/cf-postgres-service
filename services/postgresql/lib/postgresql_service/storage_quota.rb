# Copyright (c) 2009-2011 VMware, Inc.
require "pg"

module VCAP; module Services; module Postgresql; end; end; end

class VCAP::Services::Postgresql::Node

  DATA_LENGTH_FIELD = 6

  def db_size(db)
    res = @connection.exec("SELECT pg_database_size('#{db}')");
    sum = res.getvalue(0,0).to_i 
    rescue PGError => e
      @logger.warn("Postgresql exception: [#{e.err}] #{e.errstr}\n")	   
  end

  def kill_user_sessions(target_user, target_db)
     @connection.exec("SELECT pg_terminate_backend(procpid) 
                       FROM pg_stat_activity 
                       WHERE usename = '#{target_user}' AND 
                             datname = '#{target_db}'");      
  end

  def grant_write_access(db, service)
    user = service.user
    @connection.exec("GRANT ALL PRIVILEGES ON database '#{db}' to '#{user}'")
    service.quota_exceeded = false
    service.save
    rescue PGError => e
      @logger.warn("Postgresql exception: #{e}\n")
  end

  def revoke_write_access(db, service)
    user = service.user
    @connection.exec("REVOKE INSERT, UPDATE, CREATE ON '#{db}' TO '#{user}'")
    kill_user_sessions(user, db)
    service.quota_exceeded = true
    service.save
    rescue PGError => e
      @logger.warn("Postgresql exception: #{e}\n")
  end

  def fmt_db_listing(user, db, size)
    "<user: '#{user}' name: '#{db}' size: #{size}>"
  end

  def enforce_storage_quota
    #@connection.select_db('postgres')
    ProvisionedService.all.each do |service|
      db, user, quota_exceeded = service.name, service.user, service.quota_exceeded
      size = db_size(db)

      if (size >= @max_db_size) and not quota_exceeded then
        revoke_write_access(db, service)
        @logger.info("Storage quota exceeded :" + fmt_db_listing(user, db, size) +
                     " -- access revoked")
      elsif (size < @max_db_size) and quota_exceeded then
        grant_write_access(db, service)
        @logger.info("Below storage quota:" + fmt_db_listing(user, db, size) +
                     " -- access restored")
      end
    end
    rescue PGError => e
      @logger.warn("Postgresql exception: #{e}\n" +
                   e.backtrace.join("\n"))
  end

end
