package io.pravega.example.debuggability;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.commons.cli.*;

import java.net.URI;

public class StreamOperations {
    
    private final String uri;
    private String defaultScopeName = "scopeName";
    private final String streamName;
    
    private void createStream(String scopeName) {
        if(scopeName != null)
            defaultScopeName = scopeName;
        StreamManager streamManager = StreamManager.create(URI.create(uri));
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
            default:
                System.err.println("Wrong input");
        }
    }
}
