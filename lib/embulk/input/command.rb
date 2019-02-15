Embulk::JavaPlugin.register_input(
  "command", "org.embulk.input.command.CommandInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
