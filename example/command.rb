require 'yaml'

case ARGV[0]
when "setup"
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
  setup = YAML.load_file(ARGV[1])
  task_index = ARGV[2].to_i
  output_path = ARGV[3]

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
  puts "Done."
end

