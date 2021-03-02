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

public class StreamOperations {
    
    private final String uri;
    private String defaultScopeName = "scopeName";
    private final String streamName;
    
    private void createStream(String scopeName) {
        if(scopeName != null)
            defaultScopeName = scopeName;
        StreamManager streamManager = StreamManager.create(URI.create(uri));
        if(!streamManager.checkScopeExists(defaultScopeName))
            streamManager.createScope(defaultScopeName);
        StreamConfiguration streamConfiguration = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        boolean success = streamManager.createStream(defaultScopeName, streamName, streamConfiguration);
        System.out.println(success);
    }
    
    private void deleteStream(String scopeName) {
        if(scopeName != null)
            defaultScopeName = scopeName;
        StreamManager streamManager = StreamManager.create(URI.create(uri));
        if(!streamManager.getStreamInfo(defaultScopeName, streamName).isSealed())
            streamManager.sealStream(defaultScopeName, streamName);
        boolean success= streamManager.deleteStream(defaultScopeName, streamName);
        System.out.println(success);
    }
    
    private void sealStream(String scopeName) {
        if(scopeName != null)
            defaultScopeName = scopeName;
        StreamManager streamManager = StreamManager.create(URI.create(uri));
        boolean success = streamManager.sealStream(defaultScopeName, streamName);
        System.out.println(success);
    }
    
    private void truncateStream(String scopeName) {
        if(scopeName != null)
            defaultScopeName = scopeName;
        StreamManager streamManager = StreamManager.create(URI.create(uri));
        if(!streamManager.checkScopeExists(defaultScopeName))
            streamManager.createScope(defaultScopeName);
        StreamConfiguration streamConfiguration = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        streamManager.createStream(defaultScopeName, streamName, streamConfiguration);
        populateStream(defaultScopeName);
        final List<StreamCut> streamCuts = new ArrayList<>();
        final String randomId = String.valueOf(new Random(System.nanoTime()).nextInt());
        int iniEventIndex = 3;
        int endEventIndex = 8;
        // Free resources after execution.
        try (ReaderGroupManager manager = ReaderGroupManager.withScope(defaultScopeName, URI.create(uri));
             EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(defaultScopeName,
                     ClientConfig.builder().controllerURI(URI.create(uri)).build())) {

            // Create a reader group and a reader to read from the stream.
            final String readerGroupName = streamName + randomId;
            ReaderGroupConfig config = ReaderGroupConfig.builder().stream(Stream.of(defaultScopeName, streamName)).build();
            manager.createReaderGroup(readerGroupName, config);
            @Cleanup
            ReaderGroup readerGroup = manager.getReaderGroup(readerGroupName);
            @Cleanup
            EventStreamReader<String> reader = clientFactory.createReader(randomId, readerGroup.getGroupName(),
                    new JavaSerializer<>(), ReaderConfig.builder().build());

            // Read streams and create the StreamCuts during the read process.
            int eventIndex = 0;
            EventRead<String> event;
            do {
                // Here is where we create a StreamCut that points to the event indicated by the user.
                if (eventIndex == iniEventIndex || eventIndex == endEventIndex) {
                    reader.close();
                    streamCuts.add(readerGroup.getStreamCuts().get(Stream.of(defaultScopeName, streamName)));
                    reader = clientFactory.createReader(randomId, readerGroup.getGroupName(),
                            new JavaSerializer<>(), ReaderConfig.builder().build());
                }

                event = reader.readNextEvent(1000);
                eventIndex++;
            } while (event.isCheckpoint() || event.getEvent() != null);

            // If there is only the initial StreamCut, this means that the final one is the tail of the stream.
            if (streamCuts.size() == 1) {
                streamCuts.add(StreamCut.UNBOUNDED);
            }
        } catch (ReinitializationRequiredException e) {
            // We do not expect this Exception from the reader in this situation, so we leave.
            System.out.println("Non-expected reader re-initialization.");
        }
        System.out.print(streamCuts.size());
        System.out.println(streamCuts.get(0).asText());
        System.out.println(streamCuts.get(1).asText());
    }
    
    // write 10 events by default
    private void populateStream(String scopeName) {
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName,
                ClientConfig.builder().controllerURI(URI.create(uri)).build());
             EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                     new JavaSerializer<>(), EventWriterConfig.builder().build())) {
            for(int i=0;i<10;i++) {
                writer.writeEvent(String.valueOf(i));
            }
        }
    }
    
    public StreamOperations(String uri, String streamName) {
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
            case "create": streamOperations.createStream(scope);
            break;
            case "delete": streamOperations.deleteStream(scope);
            break;
            case "seal": streamOperations.sealStream(scope);
            break;
            case "truncate": streamOperations.truncateStream(scope);
            break;
            default:
                System.err.println("Wrong input");
        }
    }
}
