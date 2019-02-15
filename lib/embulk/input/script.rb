Embulk::JavaPlugin.register_input(
  "script", "org.embulk.input.script.ScriptInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
