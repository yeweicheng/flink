package org.yewc.flink.runtime.taskexecutor;

import org.apache.flink.configuration.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterConfiguration;
import org.apache.flink.runtime.entrypoint.ClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.Callable;

public class TaskManagerRunnerTest {

	protected static final Logger LOG = LoggerFactory.getLogger(TaskManagerRunnerTest.class);

	public static void main(String[] argsx) throws Exception {
		final String configDir = "D:/conf";
		final String[] args = {"--configDir", configDir};

		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

		if (maxOpenFileHandles != -1L) {
			LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
		} else {
			LOG.info("Cannot determine the maximum number of open file descriptors");
		}



		final Configuration configuration = loadConfiguration(args);
//		configuration.setString(JobManagerOptions.ADDRESS, "localhost");
//		configuration.setString(TaskManagerOptions.HOST, "localhost");
//		configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 8);

		try {
			FileSystem.initialize(configuration);
		} catch (IOException e) {
			throw new IOException("Error while setting the default " +
				"filesystem scheme from configuration.", e);
		}

		SecurityUtils.install(new SecurityConfiguration(configuration));

		try {
			SecurityUtils.getInstalledContext().runSecured(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					TaskManagerRunner taskManagerRunner = new TaskManagerRunner(configuration, ResourceID.generate());
					taskManagerRunner.start();
					return null;
				}
			});
		} catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			LOG.error("TaskManager initialization failed.", strippedThrowable);
			System.exit(1);
		}
	}

	static Configuration loadConfiguration(String[] args) throws FlinkParseException {
		final CommandLineParser<ClusterConfiguration> commandLineParser = new CommandLineParser<>(new ClusterConfigurationParserFactory());

		final ClusterConfiguration clusterConfiguration;

		try {
			clusterConfiguration = commandLineParser.parse(args);
		} catch (FlinkParseException e) {
			LOG.error("Could not parse the command line options.", e);
			commandLineParser.printHelp(TaskManagerRunner.class.getSimpleName());
			throw e;
		}

		final Configuration dynamicProperties = ConfigurationUtils.createConfiguration(clusterConfiguration.getDynamicProperties());
		return GlobalConfiguration.loadConfiguration(clusterConfiguration.getConfigDir(), dynamicProperties);
	}
}
