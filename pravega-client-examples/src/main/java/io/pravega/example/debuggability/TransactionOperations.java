package io.pravega.example.debuggability;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import org.apache.commons.cli.*;

import java.net.URI;
import java.util.Base64;
import java.util.Random;

public class TransactionOperations {

    private String controllerURI;
    private String defaultScopeName = "scopeName";
    private String defaultStreamName = "streamName";
    int numberOfSegments;
    private static final Random RANDOM = new Random();

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("u", "uri", true, "Controller URI");
        options.addOption("s", "streamName", true, "stream name");
        options.addOption("o", "operation", true, "which operation to run");
        options.addOption("scope", "scopeName", true, "scope to perform operation");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    public static void main(String[] args) throws TxnFailedException {
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
        if (cmd.hasOption("s"))
            streamName = cmd.getOptionValue("s");
        if (cmd.hasOption("scope"))
            scopeName = cmd.getOptionValue("scope");
        String uri = cmd.getOptionValue("u");
        TransactionOperations transactionOperations = new TransactionOperations(uri);
        String operation = cmd.getOptionValue("o");
        switch (operation) {
            case "fixed":
                transactionOperations.numberOfSegments = 1;
                Transaction<String> transaction = transactionOperations.createFixedSegmentsTxn(scopeName, streamName);
                transactionOperations.writeEvents(transaction, 1, 8);
                transactionOperations.commitEvents(transaction);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + operation);
        }
        return;

    }

    public TransactionOperations(String uri) {
        this.controllerURI = uri;
    }

    private Transaction<String> createFixedSegmentsTxn(String scopeName, String streamName) {
        StreamManager streamManager = StreamManager.create(URI.create(controllerURI));
        if (scopeName == null)
            scopeName = defaultScopeName;
        if (streamName == null)
            streamName = defaultStreamName;
        streamManager.createScope(scopeName);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numberOfSegments))
                .build();
        streamManager.createStream(scopeName, streamName, streamConfig);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName,
                ClientConfig.builder().controllerURI(URI.create(controllerURI)).build());
        TransactionalEventStreamWriter<String> writerTxn = clientFactory.createTransactionalEventWriter(
                streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        Transaction<String> transaction = writerTxn.beginTxn();
        return transaction;
    }

    private static String generateBigString(int size) {
        byte[] array = new byte[size];
        RANDOM.nextBytes(array);
        return Base64.getEncoder().encodeToString(array);
    }

    private Transaction<String> writeEvents(Transaction<String> transaction, int numberOfEvents,
                                            int eventSize) throws TxnFailedException {
        for (int i = 0; i < numberOfEvents; i++) {
            transaction.writeEvent(generateBigString(eventSize));
        }
        return transaction;
    }

    private void commitEvents(Transaction<String> transaction) throws TxnFailedException {
        transaction.commit();
    }
}
