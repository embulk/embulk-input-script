# Script input plugin for Embulk

Enable any developers to build Embulk input plugins using any languages.

You don't have to learn Embulk API. Your script writes a CSV file and Embulk takes it.

## Configuration

- **run**: a shell command to run (string, optional)
- **config**: contents of config.yml passed to the 1st argument of `setup` (config, optional)
- **cwd**: change path to this directory if set (string, optional)
- **env**: environment variables for the command (key-value pairs, default: `{}`)
- **try_named_pipe**: set `false` to disable named-pipe optimization (string, default: `true`)

## Developing a script

This plugin runs a given command and reads CSV file output of it.

First, you write a embulk configuration file as following:

```yaml
in:
  type: script
  run: python my_script.py
  config:
    my_config_1: value_1
out:
  type: stdout
```

With this configuration, this plugin executes your command (`python my_script.py`) as following:

1. python my_script.py **setup** config.yml _setup.yml_
2. python my_script.py **run** setup.yml **N** _output.csv_
3. python my_script.py **finish** setup.yml _next.yml_

As you see, your script runs 3 times (_italic_ is paths for write (your script writes to the paths). The others are for read).

At step 1, your script is called with **setup** as the first argument. Your script should read a config file (`config.yml`) from the path of 2nd argument, and write a YAML file (`setup.yml`) to the 3rd argument. Config file (`config.yml`) includes the contents you give in the `config:` section of the Embulk config file (`my_config_1: value_1`). Setup file (`setup.yml`) must include `tasks: N` (N is an integer) and `columns: SCHEMA` at least. See "The setup file" section bellow for details.

At step 2, your script is called with **run** as the first argument, and the YAML file written by step 1 (`setup.yml`) as the 2nd argument. Your script should write a CSV file to the 4th argument (`output.csv`). This step runs multiple times with sequence number starting from 0 as the 3rd argument (`N`). You specify number of the repeat to the `tasks` field in the setup file.

At step 3, your script is called with **finish** as the first argument, and the YAML file written by step 1 (`setup.yml`) as the 2nd argument. Your script optionally write a YAML file for the next execution to the 3rd argument.

### The setup file

At step 1, the "setup" step, your script writes a setup file (`setup.yml`) as following:

```
tasks: 1
columns:
  - {name: my_col_1, type: string}
  - {name: foo_bar, type: double}
  - {name: my_time, type: timestamp, format: "%Y-%m-%d %H:%M:%S"}
some_other_fields: anything_here
```

`tasks` gives number of tasks to run at step 2, the "run" step. If it's 3, for example, step 2 runs your script with 0, 1, and 2 as the 3rd argument.

`columns` gives schema of the data. It's necessary for embulk to be able to read the CSV file. Syntax of this field is same with the `columns` field of embulk-input-csv. You can find more details in (embulk-input-csv documents)[https://www.embulk.org/docs/built-in.html#id4].

As long as it includes `tasks` and `columns` fields, it can include any fields for your convenience.

### CSV format

CSV file written by your script must follow RFC 4180 CSV file format **without header line**.

### Example scripts

You can find script examples at [embulk/embulk-input-script/examples](https://github.com/embulk/embulk-input-script/tree/master/examples).

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## Development

### TODOs

* Packaging of script is wanted. It's something to make following action possible:

```
$ embulk-input-script-packaging --files=./ --run="./take_data.py" --output=~/embulk-input-take_data.gem
```


