package io.pravega.example.transaction;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import org.apache.commons.cli.*;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

public class TransactionPerf {
    
    private static final String scope = "scope";
    private static final int numberOfEvents = 1000; 
    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("u", "uri", true, "Controller URI");
        options.addOption("n", "noOfSegments", true, "number of segments in fixed stream");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }
    
    private static void commitTransaction (String uriString, int numberOfSegments) throws TxnFailedException {
        final URI controllerURI = URI.create(uriString);

        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numberOfSegments))
                .build();
        
        String streamName = String.valueOf(numberOfSegments) + "segmentStream";
        streamManager.createStream(scope, streamName, streamConfig);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, ClientConfig.builder().controllerURI(controllerURI).build());
        TransactionalEventStreamWriter<String> writerTxn = clientFactory.createTransactionalEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        List<Transaction<?>> transactions = new LinkedList<>();
        long start = System.nanoTime();
        Transaction<String> transaction = writerTxn.beginTxn();
        transactions.add(transaction);
        long writeTimeInitial = System.nanoTime();
        for (int i = 0; i < numberOfEvents; i++) {
            String event = "\n Transactional Publish \n";
            transaction.writeEvent(""+i, event);
        }
        long writeTime = (System.nanoTime() - writeTimeInitial)/1000000;
        long commitTimeInitial  = System.nanoTime();
        transaction.commit();
        long commitTime = (System.nanoTime() - commitTimeInitial)/1000000;
        while(transactions.stream().map(x -> x.checkStatus()).filter(txnStatus -> txnStatus != Transaction.Status.COMMITTED).findAny().isPresent());
        System.out.println(writeTime);
        System.out.println(commitTime);
    }

    public static void main(String[] args) throws TxnFailedException {
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
        final String uriString = cmd.getOptionValue("uri");
        final int numberOfSegments = Integer.parseInt(cmd.getOptionValue("n"));
        commitTransaction(uriString, numberOfSegments);
    }
}
