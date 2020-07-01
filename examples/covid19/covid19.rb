#!/usr/bin/env ruby

# COVID-19 statistics from https://thevirustracker.com/
#
# How to use:
# 1. install ruby
# 2. install embulk
# 4. install embulk-input-script       $ embulk gem install embulk-input-script
# 5. install embulk-output-postgresql  $ embulk gem install embulk-output-postgresql
# 5. run                               $ embulk run config.yml

require 'open-uri'
require 'json'
require 'yaml'
require 'csv'
require 'time'

case ARGV[0]
when "setup"  # covid19.rb setup config.yml setup.yml
  config = {
    "tasks" => 1,
    "columns" => [
      {"name" => "countrycode", "type" => "string"},
      {"name" => "date",        "type" => "timestamp", "format" => "%m/%d/%y"},
      {"name" => "cases",       "type" => "long"},
      {"name" => "deaths",      "type" => "long"},
      {"name" => "recovered",   "type" => "long"},
    ]
  }
  File.write(ARGV[2], config.to_yaml)

when "run"    # covid19.rb run setup.yml output.csv 0
  uri = URI.parse('https://thevirustracker.com/timeline/map-data.json')
  data = JSON.parse(uri.read)

  def or_zero(val)
    val.to_s.empty? ? "0" : val
  end

  CSV.open(ARGV[2], 'wb') do |csv|
    data["data"].each do |row|
      csv << [
        row["countrycode"],
        row["date"],
        or_zero(row["cases"]),
        or_zero(row["deaths"]),
        or_zero(row["recovered"]),
      ]
    end
  end

when "finish" # covid19.rb finish setup.yml

end
