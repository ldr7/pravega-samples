package io.pravega.example.debuggability;

import io.pravega.client.admin.StreamManager;
import org.apache.commons.cli.*;

import java.net.URI;

public class ScopeCreation {
    
    private final String uri;
    private final String scopeName;
    
    private ScopeCreation(String uri, String scopeName) {
        this.uri = uri;
        this.scopeName = scopeName;
    }
    
    private void createScope() {
        StreamManager streamManager = StreamManager.create(URI.create(uri));
        boolean success = streamManager.createScope(scopeName);
        System.out.println(success);
    }
    
    private void deleteScope() {
        StreamManager streamManager = StreamManager.create(URI.create(uri));
        boolean success = streamManager.deleteScope(scopeName);
        System.out.println(success);
    }
    
    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("u", "uri", true, "Controller URI");
        options.addOption("s", "scopeName", true, "scope name");
        options.addOption("d", "scopeNameToDelete", false, "delete scope");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }
    
    public static void main(String[] args) {
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
        String s = cmd.getOptionValue("scopeName", "scope");
        boolean toDelete = cmd.hasOption("d");
        ScopeCreation sc = new ScopeCreation(u, s);
        if(toDelete)
            sc.deleteScope();
        else
            sc.createScope();
    }
    
}
