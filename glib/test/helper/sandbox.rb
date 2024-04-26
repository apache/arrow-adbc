# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

require "fileutils"
require "socket"

module Helper
  class CommandRunError < StandardError
    attr_reader :commane_line
    attr_reader :output
    attr_reader :error
    def initialize(command_line, output, error)
      @command_line = command_line
      @output = output
      @error = error
      message = +"failed to run: "
      message << command_line.join(" ")
      message << "\n"
      message << "output:\n"
      message << output
      message << "error:\n"
      message << error
      super(message)
    end
  end

  module CommandRunnable
    def spawn_process(*command_line)
      env = {
        "LC_ALL" => "C",
        "PGCLIENTENCODING" => "UTF-8",
      }
      IO.pipe do |input_read, input_write|
        input_write.sync = true
        IO.pipe do |output_read, output_write|
          IO.pipe do |error_read, error_write|
            options = {
              in: input_read,
              out: output_write,
              err: error_write,
            }
            pid = spawn(env, *command_line, options)
            begin
              input_read.close
              output_write.close
              error_write.close
              yield(pid, input_write, output_read, error_read)
            ensure
              finished = false
              begin
                finished = !Process.waitpid(pid, Process::WNOHANG).nil?
              rescue SystemCallError
                # Finished
              else
                unless finished
                  Process.kill(:KILL, pid)
                  Process.waitpid(pid)
                end
              end
            end
          end
        end
      end
    end

    def read_command_output_all(input, initial_timeout: 1)
      all_output = +""
      timeout = initial_timeout
      loop do
        break unless IO.select([input], nil, nil, timeout)
        all_output << read_command_output(input)
        timeout = 0
      end
      all_output
    end

    def read_command_output(input)
      return "" unless IO.select([input], nil, nil, 0)
      begin
        data = input.readpartial(4096).gsub(/\r\n/, "\n")
        data.force_encoding("UTF-8")
        data
      rescue EOFError
        ""
      end
    end

    def run_command(*command_line)
      spawn_process(*command_line) do |pid, input_write, output_read, error_read|
        output = +""
        error = +""
        status = nil
        timeout = 1
        if block_given?
          begin
            yield(input_write, output_read, output_read)
          ensure
            input_write.close unless input_write.closed?
          end
        end
        loop do
          readables, = IO.select([output_read, error_read], nil, nil, timeout)
          if readables
            timeout = 0
            readables.each do |readable|
              if readable == output_read
                output << read_command_output(output_read)
              else
                error << read_command_output(error_read)
              end
            end
          else
            timeout = 1
          end
          _, status = Process.waitpid2(pid, Process::WNOHANG)
          break if status
        end
        output << read_command_output(output_read)
        error << read_command_output(error_read)
        unless status.success?
          raise CommandRunError.new(command_line, output, error)
        end
        [output, error]
      end
    end
  end

  class PostgreSQL
    include CommandRunnable

    attr_reader :dir
    attr_reader :host
    attr_reader :port
    attr_reader :user
    attr_reader :version
    def initialize(base_dir)
      @base_dir = base_dir
      @dir = nil
      @log_base_name = "postgresql.log"
      @log_path = nil
      @host = "127.0.0.1"
      @port = nil
      @user = "adbc"
      @version = nil
      @pid = nil
      @running = false
    end

    def running?
      @running
    end

    def initdb(db_path: "db",
               port: 15432)
      @dir = File.join(@base_dir, db_path)
      @log_path = File.join(@dir, "log", @log_base_name)
      @port = port
      run_command("initdb",
                  "--locale", "C",
                  "--encoding", "UTF-8",
                  "--username", @user,
                  "-D", @dir)
      postgresql_conf = File.join(@dir, "postgresql.conf")
      File.open(postgresql_conf, "a") do |conf|
        conf.puts("listen_addresses = '#{@host}'")
        conf.puts("port = #{@port}")
        conf.puts("logging_collector = on")
        conf.puts("log_filename = '#{@log_base_name}'")
        yield(conf) if block_given?
      end
      @version = Integer(File.read(File.join(@dir, "PG_VERSION")).chomp, 10)
    end

    def start
      begin
        run_command("pg_ctl", "start",
                    "-w",
                    "-D", @dir)
      rescue => error
        error.message << "\nPostgreSQL log:\n#{read_log}"
        raise
      end
      loop do
        begin
          TCPSocket.open(@host, @port) do
          end
        rescue SystemCallError
          sleep(0.1)
        else
          break
        end
      end
      @running = true
      pid_path = File.join(@dir, "postmaster.pid")
      if File.exist?(pid_path)
        first_line = File.readlines(pid_path, chomp: true)[0]
        begin
          @pid = Integer(first_line, 10)
        rescue ArgumentError
        end
      end
    end

    def stop
      return unless running?
      begin
        run_command("pg_ctl", "stop",
                    "-D", @dir)
      rescue CommandRunError => error
        if @pid
          Process.kill(:KILL, @pid)
          @pid = nil
          @running = false
        end
        error.message << "\nPostgreSQL log:\n#{read_log}"
        raise
      else
        @pid = nil
        @running = false
      end
    end

    def psql(db, *sqls, &block)
      command_line = [
        "psql",
        "--host", @host,
        "--port", @port.to_s,
        "--username", @user,
        "--dbname", db,
        "--echo-all",
        "--no-psqlrc",
      ]
      sqls.each do |sql|
        command_line << "--command" << sql
      end
      output, error = run_command(*command_line, &block)
      output = normalize_output(output)
      [output, error]
    end

    def read_log
      return "" unless File.exist?(@log_path)
      File.read(@log_path)
    end

    private
    def normalize_output(output)
      normalized_output = +""
      output.each_line do |line|
        case line.chomp
        when "SET", "CREATE EXTENSION"
          next
        end
        normalized_output << line
      end
      normalized_output
    end
  end

  module Sandbox
    include CommandRunnable

    class << self
      def included(base)
        base.module_eval do
          setup :setup_tmp_dir
          teardown :teardown_tmp_dir

          setup :setup_db
          teardown :teardown_db

          setup :setup_postgres
          teardown :teardown_postgres

          setup :setup_test_db
          teardown :teardown_test_db
        end
      end
    end

    def adbc_uri
      host = @postgresql.host
      port = @postgresql.port
      user = @postgresql.user
      "postgresql://#{host}:#{port}/#{@test_db_name}?user=#{user}"
    end

    def psql(db, *sqls, **options, &block)
      @postgresql.psql(db, *sqls, **options, &block)
    end

    def run_sql(*sqls, **options, &block)
      psql(@test_db_name, *sqls, **options, &block)
    end

    def setup_tmp_dir
      memory_fs = "/dev/shm"
      if File.exist?(memory_fs)
        @tmp_dir = File.join(memory_fs, "adbc")
      else
        @tmp_dir = File.join(__dir__, "tmp")
      end
      FileUtils.rm_rf(@tmp_dir)
      FileUtils.mkdir_p(@tmp_dir)
    end

    def teardown_tmp_dir
      FileUtils.rm_rf(@tmp_dir)
    end

    def setup_db
      @postgresql = PostgreSQL.new(@tmp_dir)
      begin
        @postgresql.initdb
      rescue SystemCallError => error
        omit("PostgreSQL isn't available: #{error}")
      end
    end

    def teardown_db
    end

    def start_postgres
      @postgresql.start
    end

    def stop_postgres
      @postgresql.stop
    end

    def setup_postgres
      start_postgres
    end

    def teardown_postgres
      stop_postgres if @postgresql
    end

    def create_db(postgresql, db_name)
      postgresql.psql("postgres", "CREATE DATABASE #{db_name}")
    end

    def setup_test_db
      @test_db_name = "test"
      create_db(@postgresql, @test_db_name)
      result, = run_sql("SELECT oid FROM pg_catalog.pg_database " +
                        "WHERE datname = current_database()")
      oid = result.lines[3].strip
      @test_db_dir = File.join(@postgresql.dir, "base", oid)
    end

    def teardown_test_db
    end
  end
end
