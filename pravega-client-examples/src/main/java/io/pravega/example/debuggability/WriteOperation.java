package io.pravega.example.debuggability;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.apache.commons.cli.*;

import java.net.URI;
import java.util.Base64;
import java.util.Random;

public class WriteOperation {

    private String controllerURI;
    private static final Random RANDOM = new Random();

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("u", "uri", true, "Controller URI");
        options.addOption("s", "streamName", true, "stream name");
        options.addOption("o", "operation", true, "which operation to run");
        options.addOption("scope", "scopeName", true, "scope to perform operation");
        options.addOption("n", "numberOfSegments", true, "number of fixed segments in stream");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    public static void main(String[] args) {
        String streamName = null;
        String scopeName = null;
        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            System.out.format("%s.%n", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ConsoleWriter", options);
            System.exit(1);
        }
        streamName = cmd.getOptionValue("s", "streamName");
        scopeName = cmd.getOptionValue("scope", "scopeName");
        String uri = cmd.getOptionValue("u");
        String operation = cmd.getOptionValue("o");
        int noOfSegments = Integer.parseInt(cmd.getOptionValue("n", "1"));
        switch (operation) {
            case "write":
                WriteOperation writeOperation = new WriteOperation(uri);
                writeOperation.createStream(scopeName, streamName, noOfSegments);
                EventStreamWriter writer = writeOperation.createWriter(scopeName, streamName);
                writeOperation.writeEvent(writer, "key");
                break;
            default:
                System.out.println("wrong option entered");
        }
        return;
    }

    public WriteOperation(String uri) {
        this.controllerURI = uri;
    }

    private StreamManager createStream(String scopeName, String streamName, int noOfSegments) {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        StreamManager streamManager = StreamManager.create(URI.create(controllerURI));
        streamManager.createScope(scopeName);
        streamManager.createStream(scopeName, streamName, streamConfig);
        return streamManager;
    }

    private EventStreamWriter createWriter(String scopeName, String streamName) {
        EventStreamClientFactory eventStreamClientFactory = EventStreamClientFactory.withScope(scopeName,
                ClientConfig.builder().controllerURI(URI.create(controllerURI)).build());
        EventStreamWriter<String> writer = eventStreamClientFactory.createEventWriter(streamName,
                new UTF8StringSerializer(), EventWriterConfig.builder().build());
        return writer;
    }

    private void writeEvent(EventStreamWriter writer, String routingKey) {
        writer.writeEvent(routingKey, generateBigString(1));
    }

    private static String generateBigString(int size) {
        byte[] array = new byte[size];
        RANDOM.nextBytes(array);
        return Base64.getEncoder().encodeToString(array);
    }
}
