#!/usr/bin/env ruby

require 'yaml'

case ARGV[0]
when "setup"
  # Step 1: $ ./script.rb setup config.yml setup.yml
  config = YAML.load_file(ARGV[1])
  setup_path = ARGV[2]
  puts "config: #{config.inspect}"

  setup = {
    "columns" => [
      {"name" => "s", "type" => "string"},
      {"name" => "i", "type" => "long"},
      {"name" => "f", "type" => "double"},
      {"name" => "t", "type" => "timestamp", "format" => "%Y-%m-%d %H:%M:%S"},
      {"name" => "j", "type" => "json"},
    ],
    "tasks" => 2,
  }
  File.write(setup_path, YAML.dump(setup))

when "run"
  # Step 2: $ ./script.rb run setup.yml output-0.csv 0
  setup = YAML.load_file(ARGV[1])
  output_path = ARGV[2]
  task_index = ARGV[3].to_i

  File.open(output_path, "w") do |out|
    [
      ["a", task_index, 0.1, "2019-01-01 00:00:00", ""],
      ["b", task_index, 0.2, "2019-01-01 00:00:00", ""],
      ["c", task_index, 0.3, "2019-01-01 00:00:00", ""],
    ].each do |r|
      out.puts(r.join(","))
    end
  end

when "finish"
  # Step 3: $ ./script.rb finish setup.yml
  puts "Done."
end

