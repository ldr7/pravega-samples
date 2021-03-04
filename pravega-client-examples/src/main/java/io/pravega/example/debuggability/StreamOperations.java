package io.pravega.example.debuggability;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.Cleanup;
import org.apache.commons.cli.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class StreamOperations {

    private ScheduledExecutorService executor;
    private final String uri;
    private String defaultScopeName = "scopeName";
    private final String streamName;

    private void createStream(String scopeName) {
        if (scopeName != null)
            defaultScopeName = scopeName;
        StreamManager streamManager = StreamManager.create(URI.create(uri));
        if (!streamManager.checkScopeExists(defaultScopeName))
            streamManager.createScope(defaultScopeName);
        StreamConfiguration streamConfiguration = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.fixed(1)).build();
        boolean success = streamManager.createStream(defaultScopeName, streamName, streamConfiguration);
        System.out.println(success);
    }

    private void deleteStream(String scopeName) {
        if (scopeName != null)
            defaultScopeName = scopeName;
        StreamManager streamManager = StreamManager.create(URI.create(uri));
        if (!streamManager.getStreamInfo(defaultScopeName, streamName).isSealed())
            streamManager.sealStream(defaultScopeName, streamName);
        boolean success = streamManager.deleteStream(defaultScopeName, streamName);
        System.out.println(success);
    }

    private void sealStream(String scopeName) {
        if (scopeName != null)
            defaultScopeName = scopeName;
        StreamManager streamManager = StreamManager.create(URI.create(uri));
        boolean success = streamManager.sealStream(defaultScopeName, streamName);
        System.out.println(success);
    }

    private void truncateStream(String scopeName) {
        if (scopeName != null)
            defaultScopeName = scopeName;
        StreamManager streamManager = StreamManager.create(URI.create(uri));
        if (!streamManager.checkScopeExists(defaultScopeName))
            streamManager.createScope(defaultScopeName);
        StreamConfiguration streamConfiguration = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.fixed(1)).build();
        streamManager.createStream(defaultScopeName, streamName, streamConfiguration);
        populateStream(defaultScopeName);
        final List<StreamCut> streamCuts = new ArrayList<>();
        // Free resources after execution.
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(defaultScopeName, URI.create(uri))) {
            ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(defaultScopeName, streamName))
                    .disableAutomaticCheckpoints()
                    .build();
            readerGroupManager.createReaderGroup("readerGroup", readerGroupConfig);
            @Cleanup
            ReaderGroup readerGroup = readerGroupManager.getReaderGroup("readerGroup");
            try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(defaultScopeName,
                    ClientConfig.builder().controllerURI(URI.create(uri)).build());
                 EventStreamReader<String> reader = clientFactory.createReader("reader",
                         "readerGroup",
                         new JavaSerializer<>(),
                         ReaderConfig.builder().build())) {
                String event = reader.readNextEvent(1000).getEvent();
                while (event != null) {
                    System.out.println(event);
                    if (event.equals(String.valueOf(3)) || event.equals(String.valueOf(8)))
                        readerGroup.generateStreamCuts(executor);
                    event = reader.readNextEvent(1000).getEvent();
                }
                System.out.println("number of elements in streamCut map: " + readerGroup.getStreamCuts().size());
                System.out.println(readerGroup.getStreamCuts().get(Stream.of(defaultScopeName, streamName)));
                boolean success = streamManager.truncateStream(defaultScopeName, streamName,
                        readerGroup.getStreamCuts().get(Stream.of(defaultScopeName, streamName)));
                System.out.println(success);
                readStream(defaultScopeName);
            }
        }
    }
    
    private void readStream(String scopeName) {
        System.out.println("read stream method");
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, URI.create(uri))) {
            ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(scopeName, streamName))
                    .disableAutomaticCheckpoints()
                    .build();
            readerGroupManager.createReaderGroup("readerGroup1", readerGroupConfig);
            try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName,
                    ClientConfig.builder().controllerURI(URI.create(uri)).build());
                 EventStreamReader<String> reader = clientFactory.createReader("reader1",
                         "readerGroup1",
                         new JavaSerializer<>(),
                         ReaderConfig.builder().build())) {
                String event = reader.readNextEvent(1000).getEvent();
                while (event != null) {
                    System.out.println(event);
                    event = reader.readNextEvent(1000).getEvent();
                }
            }
        }
    }

    // write 10 events by default
    private void populateStream(String scopeName) {
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName,
                ClientConfig.builder().controllerURI(URI.create(uri)).build());
             EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                     new JavaSerializer<>(), EventWriterConfig.builder().build())) {
            for (int i = 0; i < 10; i++) {
                writer.writeEvent(String.valueOf(i));
            }
        }
    }

    public StreamOperations(String uri, String streamName) {
        this.executor = new ScheduledThreadPoolExecutor(1);
        this.uri = uri;
        this.streamName = streamName;
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("u", "uri", true, "Controller URI");
        options.addOption("s", "streamName", true, "stream name");
        options.addOption("o", "operation", true, "operation to perform");
        options.addOption("scope", "scopeName", true, "scope to perform operation");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    static public void main(String[] args) {
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
        String u = cmd.getOptionValue("uri");
        String scope = cmd.getOptionValue("scopeName");
        String stream = cmd.getOptionValue("streamName");
        String operation = cmd.getOptionValue("operation");
        StreamOperations streamOperations = new StreamOperations(u, stream);
        switch (operation) {
            case "create":
                streamOperations.createStream(scope);
                break;
            case "delete":
                streamOperations.deleteStream(scope);
                break;
            case "seal":
                streamOperations.sealStream(scope);
                break;
            case "truncate":
                streamOperations.truncateStream(scope);
                break;
            default:
                System.err.println("Wrong input");
        }
    }
}
