package org.yewc.flink.runtime.entrypoint;

import org.apache.flink.configuration.*;
import org.apache.flink.runtime.entrypoint.*;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandaloneSessionTest {

	protected static final Logger LOG = LoggerFactory.getLogger(StandaloneSessionTest.class);

	public static void main(String[] argsx) throws Exception {
		final String configDir = "D:/conf";
		final int restPort = 1234;
		final String key = "key";
		final String value = "value";
		final String arg1 = "arg1";
		final String arg2 = "arg2";
		final String[] args = {"--configDir", configDir, "--executionMode", "cluster", "--host", "localhost",  "-r", String.valueOf(restPort), String.format("-D%s=%s", key, value), arg1, arg2};

		CommandLineParser<EntrypointClusterConfiguration> commandLineParser = new CommandLineParser<>(new EntrypointClusterConfigurationParserFactory());
		EntrypointClusterConfiguration entrypointClusterConfiguration = null;

		try {
			entrypointClusterConfiguration = commandLineParser.parse(args);
		} catch (FlinkParseException e) {
			LOG.error("Could not parse command line arguments {}.", args, e);
			commandLineParser.printHelp(StandaloneSessionClusterEntrypoint.class.getSimpleName());
			System.exit(1);
		}

		Configuration configuration = loadConfiguration(entrypointClusterConfiguration);

		StandaloneSessionClusterEntrypoint entrypoint = new StandaloneSessionClusterEntrypoint(configuration);

		ClusterEntrypoint.runClusterEntrypoint(entrypoint);
	}

	public static Configuration loadConfiguration(EntrypointClusterConfiguration entrypointClusterConfiguration) {
		final Configuration dynamicProperties = ConfigurationUtils.createConfiguration(entrypointClusterConfiguration.getDynamicProperties());
		final Configuration configuration = GlobalConfiguration.loadConfiguration(entrypointClusterConfiguration.getConfigDir(), dynamicProperties);

		final int restPort = entrypointClusterConfiguration.getRestPort();

		if (restPort >= 0) {
			configuration.setInteger(RestOptions.PORT, restPort);
		}

		final String hostname = entrypointClusterConfiguration.getHostname();

		if (hostname != null) {
			configuration.setString(JobManagerOptions.ADDRESS, hostname);
		}

		return configuration;
	}
}
