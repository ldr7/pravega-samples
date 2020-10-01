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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
        long writeTime = 0;
        long commitTime = 0;
        long beginCallTime = 0;
        long origin = 0;
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
        long endTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(5L, TimeUnit.MINUTES);
        while(System.nanoTime() < endTime) {
            long beginTime = System.nanoTime();
            Transaction<String> transaction = writerTxn.beginTxn();
            if (beginCallTime == 0){
                origin = System.nanoTime();   
            }
            beginCallTime += System.nanoTime() - beginTime;
            transactions.add(transaction);
            long writeTimeInitial = System.nanoTime();
            for (int i = 0; i < numberOfEvents; i++) {
                String event = "\n Transactional Publish \n";
                transaction.writeEvent("" + i, event);
            }
            writeTime += System.nanoTime() - writeTimeInitial;
            long commitTimeInitial = System.nanoTime();
            transaction.commit();
            commitTime += System.nanoTime() - commitTimeInitial;
        }
        while (!transactions.isEmpty()) {
            transactions = transactions.stream().filter(
                    x -> x.checkStatus() != Transaction.Status.COMMITTED).collect(Collectors.toList());
        }
        int noOfTransactions = transactions.size();
        long totalTime = System.nanoTime() - origin;
        long waitTime = totalTime - (beginCallTime + writeTime + commitTime);
        System.out.println("beginCallTime: " + beginCallTime/noOfTransactions);
        System.out.println("writeTime: " + writeTime/noOfTransactions);
        System.out.println("commitTime: " + commitTime/noOfTransactions);
        System.out.println("Average wait time: " + waitTime/noOfTransactions);
        System.out.println("total time: " + totalTime);
        System.out.println("Number of Transactions: " + noOfTransactions);
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
