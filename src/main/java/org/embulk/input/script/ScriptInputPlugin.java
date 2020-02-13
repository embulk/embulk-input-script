package org.embulk.input.script;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.escape.CharEscaperBuilder;
import com.google.common.escape.Escaper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.ModelManager;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParseException;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.util.Timestamps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ScriptInputPlugin
        implements InputPlugin
{
    public interface PluginTask extends Task {
        @Config("run")
        public String getCommand();

        @Config("config")
        @ConfigDefault("{}")
        public ConfigSource getConfig();

        @Config("cwd")
        @ConfigDefault("\"\"")
        public String getCwd();

        @Config("env")
        @ConfigDefault("{}")
        public Map<String, String> getEnv();

        @Config("try_named_pipe")
        @ConfigDefault("true")
        public boolean getTryNamedPipe();

        @ConfigInject
        public ConfigLoader getConfigLoader();

        public String getTempDir();
        public void setTempDir(String path);

        public ConfigSource getSetupConfig();
        public void setSetupConfig(ConfigSource setupConfig);
    }

    public interface SetupTask extends Task, TimestampParser.Task {
        @Config("columns")
        SchemaConfig getSchemaConfig();

        @Config("tasks")
        @ConfigDefault("1")
        int getTasks();
    }

    static List<String> buildShell()
    {
        String osName = System.getProperty("os.name");
        if(osName.indexOf("Windows") >= 0) {
            return ImmutableList.of("PowerShell.exe", "-Command");
        } else {
            return ImmutableList.of("sh", "-c");
        }
    }

    public static final Escaper SHELL_ESCAPER = new CharEscaperBuilder()
        .addEscape('\'', "'\"'\"'")
        .addEscape(' ', "\\ ")
        .toEscaper();

    private static Path resolveOutPath(Path tempDir, int taskIndex) {
        return tempDir.resolve("output-" + taskIndex + ".csv");
    }

    private static Path resolveConfigPath(Path tempDir) {
        return tempDir.resolve("config.yml");
    }

    private static Path resolveSetupPath(Path tempDir) {
        return tempDir.resolve("setup.yml");
    }

    private static final Logger logger = LoggerFactory.getLogger(ScriptInputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);
        Path tempDir;
        Path configPath;
        try {
            tempDir = Files.createTempDirectory("embulk-input-script-");
            task.setTempDir(tempDir.toString());

            // create /tmp/config.yml
            configPath = resolveConfigPath(tempDir);
            writeConfigFile(configPath, task.getConfig());
        } catch (IOException ex) {
            throw new ConfigException("Failed to create config.yml file", ex);
        }

        // $ <command> setup /tmp/config.yml /tmp/setup.yml
        Path setupPath = resolveSetupPath(tempDir);
        try {
            int ecode = runCommand(task.getCwd(), task.getEnv(), task.getCommand(),
                    ImmutableList.of(
                        "setup",
                        configPath.toString(),
                        setupPath.toString()));
            if (ecode != 0) {
                throw new ConfigException("Setup command exited with error code " + ecode);
            }
        } catch (IOException ex) {
            throw new ConfigException("Failed to start setup command", ex);
        }

        // read /tmp/setup.yml
        ConfigSource setupConfig;
        try {
            setupConfig = task.getConfigLoader().fromYamlFile(setupPath.toFile());
        } catch (IOException ex) {
            throw new ConfigException("Setup command didn't create appropriate setup.yml", ex);
        }
        task.setSetupConfig(setupConfig);
        SetupTask setup = setupConfig.loadConfig(SetupTask.class);

        Schema schema = setup.getSchemaConfig().toSchema();
        int tasks = setup.getTasks();
        return resume(task.dump(), schema, tasks, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control) {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        Path tempDir = Paths.get(task.getTempDir());

        // Repeat run for taskCount times
        control.run(taskSource, schema, taskCount);

        // $ <command> finish /tmp/setup.yml
        Path setupPath = resolveSetupPath(tempDir);
        try {
            int ecode = runCommand(task.getCwd(), task.getEnv(), task.getCommand(),
                    ImmutableList.of(
                        "finish",
                        setupPath.toString()));
            if (ecode != 0) {
                throw new ConfigException("Finish command exited with error code " + ecode);
            }
        } catch (IOException ex) {
            throw new ConfigException("Failed to start finish command", ex);
        }

        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports) {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        Path tempDir = Paths.get(task.getTempDir());
        for (int i = 0; i < taskCount; i++) {
            Path outPath = resolveOutPath(tempDir, i);
            try {
                Files.deleteIfExists(outPath);
            }
            catch (IOException ex) {
                // ignore
            }
        }
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        SetupTask setup = task.getSetupConfig().loadConfig(SetupTask.class);

        Path tempDir = Paths.get(task.getTempDir());
        Path setupPath = resolveSetupPath(tempDir);

        TimestampParser[] timestampParsers =
            Timestamps.newTimestampColumnParsers(setup, setup.getSchemaConfig());

        Map<String, String> env = new HashMap<>(task.getEnv());
        env.put("INDEX", Integer.toString(taskIndex));  // compatible with embulk-output-command

        Path outPath = resolveOutPath(tempDir, taskIndex);
        int ecode;
        try {
            // $ <command> run /tmp/setup.yml <output-taskIndex.csv> <taskIndex>
            if (task.getTryNamedPipe() && tryToCreateNamedPipe(outPath)) {
                Process proc = startCommand(task.getCwd(), task.getEnv(), task.getCommand(),
                        ImmutableList.of(
                            "run",
                            setupPath.toString(),
                            outPath.toString(),
                            Integer.toString(taskIndex)));
                try (InputStream in = Files.newInputStream(outPath)) {
                    readRecords(in, output, schema, taskIndex, timestampParsers);
                } finally {
                    ecode = waitFor(proc);
                }
            } else {
                Files.deleteIfExists(outPath);
                ecode = runCommand(task.getCwd(), task.getEnv(), task.getCommand(),
                        ImmutableList.of(
                            "run",
                            setupPath.toString(),
                            outPath.toString(),
                            Integer.toString(taskIndex)));
                try (InputStream in = Files.newInputStream(outPath)) {
                    readRecords(in, output, schema, taskIndex, timestampParsers);
                }
            }

            if (ecode != 0) {
                throw new ConfigException("Run command exited with error code " + ecode);
            }
        }
        catch (IOException ex) {
            throw new DataException("Failed to start run command", ex);
        }

        return Exec.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    private void writeConfigFile(Path path, ConfigSource config)
        throws IOException
    {
        ModelManager modelManager = Exec.getModelManager();
        Object object = modelManager.readObject(Object.class, modelManager.writeObject(config));
        String yamlString = new Yaml().dump(object);
        try (BufferedWriter writer = Files.newBufferedWriter(path, UTF_8)) {
            writer.write(yamlString);
        }
    }

    private ConfigSource readSetupFile(ConfigLoader configLoader, Path setupPath)
        throws IOException
    {
        return configLoader.fromYamlFile(setupPath.toFile());
    }

    private Process startCommand(String cwd, Map<String, String> env,
            String shellCommand, List<String> args)
        throws IOException
    {
        logger.info("Running command: {} {}", shellCommand, args);

        // $ sh -c "shellCommand \"@args\""
        List<String> sh = new ArrayList<>();
        sh.addAll(buildShell());
        {
            StringBuilder esc = new StringBuilder();
            esc.append(shellCommand);
            for (String cmd : args) {
                esc.append(" ");
                esc.append(SHELL_ESCAPER.escape(cmd));
            }
            sh.add(esc.toString());
        }

        ProcessBuilder pb = new ProcessBuilder(sh);
        if (!cwd.equals("")) {
            pb.directory(new File(cwd));
        }
        pb.environment().putAll(env);
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectInput(ProcessBuilder.Redirect.PIPE);

        Process proc = pb.start();
        proc.getOutputStream().close();

        return proc;
    }

    private int runCommand(String cwd, Map<String, String> env,
            String shellCommand, List<String> cmdline)
        throws IOException
    {
        return waitFor(startCommand(cwd, env, shellCommand, cmdline));
    }

    private boolean tryToCreateNamedPipe(Path path)
    {
        try {
            ProcessBuilder pb = new ProcessBuilder("mkfifo", path.toString());
            pb.inheritIO();
            Process proc = pb.start();
            int ecode = waitFor(proc);
            return ecode == 0;
        }
        catch (IOException ex) {
            return false;
        }
    }

    private int waitFor(Process proc)
    {
        try {
            return proc.waitFor();
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
    }

    private static final ImmutableSet<String> TRUE_STRINGS =
        ImmutableSet.of(
                "true", "True", "TRUE",
                "yes", "Yes", "YES",
                "t", "T", "y", "Y",
                "on", "On",
                "ON", "1");

    private static DateTimeFormatter TIMESTAMP_PARSER = DateTimeFormatter.ISO_INSTANT;

    static class CsvRecordValidateException extends DataException {
        CsvRecordValidateException(String message) {
            super(message);
        }

        CsvRecordValidateException(Throwable cause) {
            super(cause);
        }
    }

    private void readRecords(InputStream in, PageOutput output, Schema schema, int taskIndex, TimestampParser[] timestampParsers)
    {
        JsonParser jsonParser = new JsonParser();

        try (InputStreamReader reader = new InputStreamReader(in, UTF_8); CSVReader csv = new CSVReaderBuilder(reader).withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS).build()) {
            Iterator<String[]> ite = csv.iterator();
            try (PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
                while (ite.hasNext()) {
                    readRecord(ite.next(), pageBuilder, schema, jsonParser, timestampParsers);
                    pageBuilder.addRecord();
                }
                pageBuilder.finish();
            }

        }
        catch (IOException ex) {
            throw new DataException("Failed to parse output file " + taskIndex, ex);
        }
    }

    private void readRecord(String[] record, PageBuilder pageBuilder, Schema schema,
            JsonParser jsonParser, TimestampParser[] timestampParsers)
    {
        if (record.length != schema.getColumnCount()) {
            throw new CsvRecordValidateException("Invalid number of columns (expected " + schema.getColumnCount() + " but got " + record.length + ")");
        }

        schema.visitColumns(new ColumnVisitor() {
            public void booleanColumn(Column column) {
                String v = record[column.getIndex()];
                if (v == null) {
                    pageBuilder.setNull(column);
                } else {
                    pageBuilder.setBoolean(column, TRUE_STRINGS.contains(v));
                }
            }

            public void longColumn(Column column) {
                String v = record[column.getIndex()];
                if (v == null) {
                    pageBuilder.setNull(column);
                } else {
                    try {
                        pageBuilder.setLong(column, Long.parseLong(v));
                    } catch (NumberFormatException e) {
                        throw new CsvRecordValidateException(e);
                    }
                }
            }

            public void doubleColumn(Column column) {
                String v = record[column.getIndex()];
                if (v == null) {
                    pageBuilder.setNull(column);
                } else {
                    try {
                        pageBuilder.setDouble(column, Double.parseDouble(v));
                    } catch (NumberFormatException e) {
                        throw new CsvRecordValidateException(e);
                    }
                }
            }

            public void stringColumn(Column column) {
                String v = record[column.getIndex()];
                if (v == null) {
                    pageBuilder.setNull(column);
                } else {
                    pageBuilder.setString(column, v);
                }
            }

            public void timestampColumn(Column column) {
                String v = record[column.getIndex()];
                if (v == null) {
                    pageBuilder.setNull(column);
                } else {
                    try {
                        pageBuilder.setTimestamp(column, timestampParsers[column.getIndex()].parse(v));
                    } catch (TimestampParseException e) {
                        throw new CsvRecordValidateException(e);
                    }
                }
            }

            public void jsonColumn(Column column) {
                String v = record[column.getIndex()];
                if (v == null) {
                    pageBuilder.setNull(column);
                } else {
                    try {
                        pageBuilder.setJson(column, jsonParser.parse(v));
                    } catch (JsonParseException e) {
                        throw new CsvRecordValidateException(e);
                    }
                }
            }
        });
    }
}
