package io.pravega.example.transaction;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.Data;
import org.apache.commons.cli.*;

import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TransactionPerf {
    private static final Random RANDOM = new Random();
    private static final String scope = "scope";

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("u", "uri", true, "Controller URI");
        options.addOption("n", "noOfSegments", true, "number of segments in fixed stream");
        options.addOption("e", "noOfEvents", true, "number of events to write");
        options.addOption("s", "useSingleRoutingKey", false, "use single routing key");
        options.addOption("t", "timeInMinutes", true, "time in minutes");
        options.addOption("d", "delaysBetweenExperiments", true, "delaysBetweenExperiments");
        options.addOption("i", "numberOfIterations", true, "numberOfIterations");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }

    private static String generateBigString(int size) {
        byte[] array = new byte[size];
        RANDOM.nextBytes(array);
        return Base64.getEncoder().encodeToString(array);
    }

    private static void commitTransaction(String uriString, List<Integer> values, int numberOfIterations,
                                          int numberOfEvents, int size, boolean singleRoutingKey,
                                          int timeInMinutes,
                                          int delayInMinutes) throws TxnFailedException, InterruptedException {
        TreeMap<Integer, Long> writeTimeMap = new TreeMap<Integer, Long>();
        TreeMap<Integer, Long> commitTimeMap = new TreeMap<Integer, Long>();
        TreeMap<Integer, Long> beginCallMap = new TreeMap<Integer, Long>();
        TreeMap<Integer, Integer> noOfTransactionsMap = new TreeMap<Integer, Integer>();
        TreeMap<Integer, Long> avgWaitTimeMap = new TreeMap<Integer, Long>();
        final URI controllerURI = URI.create(uriString);
        Map<Integer, Result> result = new HashMap<>();
        for (int numberOfSegments : values) {
            writeTimeMap.clear();
            commitTimeMap.clear();
            beginCallMap.clear();
            noOfTransactionsMap.clear();
            avgWaitTimeMap.clear();
            for (int i = 0; i < numberOfIterations; i++) {
                long writeTime = 0;
                long commitTime = 0;
                long beginCallTime = 0;
                long origin = 0;
                try (StreamManager streamManager = StreamManager.create(controllerURI)) {
                    streamManager.createScope(scope);
                    StreamConfiguration streamConfig = StreamConfiguration.builder()
                            .scalingPolicy(ScalingPolicy.fixed(numberOfSegments))
                            .build();
                    String streamName = numberOfSegments + "segmentsStream";
                    streamManager.createStream(scope, streamName, streamConfig);
                    EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                            ClientConfig.builder().controllerURI(controllerURI).build());
                    TransactionalEventStreamWriter<String> writerTxn = clientFactory.createTransactionalEventWriter(
                            streamName, new JavaSerializer<>(),
                            EventWriterConfig.builder().build());
                    List<Transaction<?>> transactions = new LinkedList<>();
                    long endTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeInMinutes, TimeUnit.MINUTES);
                    String event = generateBigString(size);
                    while (System.nanoTime() < endTime) {
                        long beginTime = System.nanoTime();
                        Transaction<String> transaction = writerTxn.beginTxn();
                        if (beginCallTime == 0) {
                            origin = System.nanoTime();
                        }
                        beginCallTime += System.nanoTime() - beginTime;
                        transactions.add(transaction);
                        long writeTimeInitial = System.nanoTime();
                        for (int j = 0; j < numberOfEvents; j++) {
                            String routingKey = singleRoutingKey ? "a" : "" + j;
                            transaction.writeEvent(routingKey, event);
                        }
                        writeTime += System.nanoTime() - writeTimeInitial;
                        long commitTimeInitial = System.nanoTime();
                        transaction.commit();
                        commitTime += System.nanoTime() - commitTimeInitial;
                    }
                    int noOfTransactions = transactions.size();
                    while (!transactions.isEmpty()) {
                        transactions = transactions.stream().filter(
                                x -> x.checkStatus() != Transaction.Status.COMMITTED).collect(Collectors.toList());
                    }
                    long totalTime = System.nanoTime() - origin;
                    long waitTime = totalTime - (beginCallTime + writeTime + commitTime);
                    System.out.println("beginCallTime: " + beginCallTime / noOfTransactions);
                    System.out.println("writeTime: " + writeTime / noOfTransactions);
                    System.out.println("commitTime: " + commitTime / noOfTransactions);
                    System.out.println("Average wait time: " + waitTime / noOfTransactions);
                    System.out.println("additional wait time: " + waitTime);
                    System.out.println("total time: " + totalTime);
                    System.out.println("Number of Transactions: " + noOfTransactions);
                    Result res = new Result(writeTime, commitTime, beginCallTime, noOfTransactions, waitTime);
                    int count = i;
                    result.compute(numberOfSegments, (x, y) -> {
                        if (y == null) {
                            return res;
                        } else {
                            return y.average(count, res);
                        }
                    });
                    writeTimeMap.put(i, writeTime / noOfTransactions);
                    beginCallMap.put(i, beginCallTime / noOfTransactions);
                    commitTimeMap.put(i, commitTime / noOfTransactions);
                    avgWaitTimeMap.put(i, waitTime / noOfTransactions);
                    noOfTransactionsMap.put(i, noOfTransactions);
                    Thread.sleep(delayInMinutes == 0 ? 10 * 1000 : delayInMinutes * 60 * 1000);
                }
            }
            writeTimeMap = (TreeMap<Integer, Long>) valueSort(writeTimeMap);
            ArrayList<Long> writeTimeMapValuesList = new ArrayList<>(writeTimeMap.values());
            beginCallMap = (TreeMap<Integer, Long>) valueSort(beginCallMap);
            ArrayList<Long> beginCallMapValuesList = new ArrayList<>(beginCallMap.values());
            commitTimeMap = (TreeMap<Integer, Long>) valueSort(commitTimeMap);
            ArrayList<Long> commitTimeMapValuesList = new ArrayList<>(commitTimeMap.values());
            avgWaitTimeMap = (TreeMap<Integer, Long>) valueSort(avgWaitTimeMap);
            ArrayList<Long> avgWaitTimeMapList = new ArrayList<>(avgWaitTimeMap.values());
            noOfTransactionsMap = (TreeMap<Integer, Integer>) valueSort(noOfTransactionsMap);
            ArrayList<Integer> noOfTransactionsValuesList = new ArrayList<Integer>(noOfTransactionsMap.values());
            System.out.println("Percentile Printing");
            System.out.println("Number of segments" + numberOfSegments);
            System.out.println("75th percentile number of write time" + writeTimeMapValuesList.get((int)0.75*numberOfIterations));
            System.out.println("75th percentile number of avg wait time" + avgWaitTimeMapList.get((int)0.75*numberOfIterations));
            System.out.println("75th percentile number of begin call time" + beginCallMapValuesList.get((int)0.75*numberOfIterations));
            System.out.println("75th percentile number of commit time" + commitTimeMapValuesList.get((int)0.75*numberOfIterations));
            System.out.println("75th percentile number of transactions" + noOfTransactionsValuesList.get((int)0.75*numberOfIterations));
        }

        System.out.println("-------------------------------- final result ------------------------------");
        result.forEach((x, y) -> {
            System.out.println("number of segments = : " + x);
            System.out.println("beginCallTime: " + y.beginCallTime);
            System.out.println("writeTime: " + y.writeTime);
            System.out.println("commitTime: " + y.commitTime);
            System.out.println("Average wait time: " + y.additionalDelay / y.noOfTransactions);
            System.out.println("additional wait time: " + y.additionalDelay);
            System.out.println("Number of Transactions: " + y.noOfTransactions);
            System.out.println("--------------------------------------------------------------");
        });
    }

    public static void main(String[] args) throws TxnFailedException, InterruptedException {
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
        final List<Integer> numberOfSegments = Arrays.stream(cmd.getOptionValue("n", "1").split(","))
                .map(Integer::parseInt).collect(Collectors.toList());
        final int numberOfIterations = Integer.parseInt(cmd.getOptionValue("i", "30"));
        final int numberOfEvents = Integer.parseInt(cmd.getOptionValue("e", "1000"));
        final int eventSize = Integer.parseInt(cmd.getOptionValue("s", "1024"));
        final int timeInMinutes = Integer.parseInt(cmd.getOptionValue("t", "5"));
        final boolean singleRoutingKey = cmd.hasOption("s");
        final int delayInMinutes = Integer.parseInt(cmd.getOptionValue("d", "5"));

        commitTransaction(uriString, numberOfSegments, numberOfIterations, numberOfEvents, eventSize, singleRoutingKey,
                timeInMinutes, delayInMinutes);
    }

    public static <K, V extends Comparable<V> > Map<K, V>
    valueSort(final Map<K, V> map)
    {
        // Static Method with return type Map and 
        // extending comparator class which compares values 
        // associated with two keys 
        Comparator<K> valueComparator = new Comparator<K>()
        {
            public int compare(K k1, K k2)
            {
                int comp = map.get(k1).compareTo(map.get(k2));
                if (comp == 0)
                    return 1;
                else
                    return comp;
            }
        };
        Map<K, V> sorted = new TreeMap<K, V>(valueComparator);
        sorted.putAll(map);
        return sorted;
    }

    @Data
    private static class Result {
        private final long writeTime;
        private final long commitTime;
        private final long beginCallTime;
        private final int noOfTransactions;
        private final long additionalDelay;

        public Result average(int count, Result res) {
            long writeAvg = (this.writeTime * count + res.writeTime) / count + 1;
            long commitTimeAvg = (this.commitTime * count + res.commitTime) / count + 1;
            long beginCallTimeAvg = (this.beginCallTime * count + res.beginCallTime) / count + 1;
            int noOfTransactionsAvg = (this.noOfTransactions * count + res.noOfTransactions) / count + 1;
            long additionalDelayAvg = (this.additionalDelay * count + res.additionalDelay) / count + 1;
            return new Result(writeAvg, commitTimeAvg, beginCallTimeAvg, noOfTransactionsAvg, additionalDelayAvg);
        }
    }
}