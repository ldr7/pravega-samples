package io.pravega.example.transaction;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.Data;
import org.apache.commons.cli.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TransactionPerf {
    private static final Random RANDOM = new Random();
    private static final String scope = "scope";
    private static final String fileName = "TxnPerfRuns.txt";

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
                                          int delayInMinutes) throws TxnFailedException, InterruptedException,
                                                                     IOException {
        final URI controllerURI = URI.create(uriString);
        Map<Integer, Result> result = new HashMap<>();
        for (int numberOfSegments : values) {
            try (FileWriter fw = new FileWriter(fileName, true);
                 BufferedWriter bw = new BufferedWriter(fw);
                 PrintWriter out = new PrintWriter(bw)) {
                out.println("NUMBER OF SEGMENTS: " + numberOfSegments);
            }
            TreeMap<Integer, Long> writeTimeMap = new TreeMap<Integer, Long>();
            TreeMap<Integer, Long> commitTimeMap = new TreeMap<Integer, Long>();
            TreeMap<Integer, Long> beginCallMap = new TreeMap<Integer, Long>();
            TreeMap<Integer, Integer> noOfTransactionsMap = new TreeMap<Integer, Integer>();
            TreeMap<Integer, Long> avgWaitTimeMap = new TreeMap<Integer, Long>();
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
                    try (FileWriter fw = new FileWriter(fileName, true);
                         BufferedWriter bw = new BufferedWriter(fw);
                         PrintWriter out = new PrintWriter(bw)) {
                        out.println(
                                "beginCallTime: " + Duration.ofNanos(beginCallTime / noOfTransactions).toMillis());
                        out.println("writeTime: " + Duration.ofNanos(writeTime / noOfTransactions).toMillis());
                        out.println("commitTime: " + Duration.ofNanos(commitTime / noOfTransactions).toMillis());
                        out.println("Average wait time: " + Duration.ofNanos(waitTime / noOfTransactions).toMillis());
                        out.println("additional wait time: " + Duration.ofNanos(waitTime).toMillis());
                        out.println("total time: " + Duration.ofNanos(totalTime).toMillis());
                        out.println("Number of Transactions: " + noOfTransactions);
                    }
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
                    runningPercentile(writeTimeMap, beginCallMap, commitTimeMap, avgWaitTimeMap, noOfTransactionsMap);
                    Thread.sleep(delayInMinutes == 0 ? 10 * 1000 : delayInMinutes * 60 * 1000);
                }
            }
        }
        try (FileWriter fw = new FileWriter(fileName, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println("-------------------------------- final result ------------------------------");
            result.forEach((x, y) -> {
                out.println("number of segments = : " + x);
                out.println("beginCallTime: " + Duration.ofNanos(y.beginCallTime).toMillis());
                out.println("writeTime: " + Duration.ofNanos(y.writeTime).toMillis());
                out.println("commitTime: " + Duration.ofNanos(y.commitTime).toMillis());
                out.println(
                        "Average wait time: " + Duration.ofNanos(y.additionalDelay / y.noOfTransactions).toMillis());
                out.println("additional wait time: " + Duration.ofNanos(y.additionalDelay).toMillis());
                out.println("Number of Transactions: " + y.noOfTransactions);
                out.println("--------------------------------------------------------------");
            });
        }
    }

    public static void main(String[] args) throws TxnFailedException, InterruptedException, IOException {
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
        createFile();
        commitTransaction(uriString, numberOfSegments, numberOfIterations, numberOfEvents, eventSize, singleRoutingKey,
                timeInMinutes, delayInMinutes);
    }

    public static <K, V extends Comparable<V>> Map<K, V>
    valueSort(final Map<K, V> map) {
        // Static Method with return type Map and 
        // extending comparator class which compares values 
        // associated with two keys 
        Comparator<K> valueComparator = new Comparator<K>() {
            public int compare(K k1, K k2) {
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

    private static void createFile() {
        try {
            FileWriter fw = new FileWriter(fileName);
            fw.close();
        } catch (IOException e) {
            System.out.println("Exception in file creation");
            e.printStackTrace();
        }
    }

    private static void runningPercentile(TreeMap<Integer, Long> writeTimeMap, TreeMap<Integer, Long> beginCallMap,
                                          TreeMap<Integer, Long> commitTimeMap, TreeMap<Integer, Long> avgWaitTimeMap,
                                          TreeMap<Integer, Integer> noOfTransactionsMap) throws IOException {
        int numberOfIterations = noOfTransactionsMap.size();
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
        try (FileWriter fw = new FileWriter(fileName, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxPercentile Printingxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
            out.println("Percentile 50th --------------");
            if (numberOfIterations % 2 == 0) {
                out.println("50th percentile number of write time: " + Duration.ofNanos(
                        (writeTimeMapValuesList.get((int) (0.50 * numberOfIterations)) + writeTimeMapValuesList.get(
                                ((int) (0.50 * numberOfIterations)) - 1)) / 2).toMillis());
                out.println("50th percentile number of avg wait time: " + Duration.ofNanos(
                        (avgWaitTimeMapList.get((int) (0.50 * numberOfIterations)) + avgWaitTimeMapList.get(
                                ((int) (0.50 * numberOfIterations)) - 1)) / 2).toMillis());
                out.println("50th percentile number of begin call time: " + Duration.ofNanos(
                        (beginCallMapValuesList.get((int) (0.50 * numberOfIterations)) + beginCallMapValuesList.get(
                                ((int) (0.50 * numberOfIterations)) - 1)) / 2).toMillis());
                out.println("50th percentile number of commit time: " + Duration.ofNanos(
                        (commitTimeMapValuesList.get((int) (0.50 * numberOfIterations)) + commitTimeMapValuesList.get(
                                ((int) (0.50 * numberOfIterations)) - 1)) / 2).toMillis());
                out.println("50th percentile number of transactions: " + (noOfTransactionsValuesList.get(
                        (int) (0.50 * numberOfIterations)) + noOfTransactionsValuesList.get(
                        ((int) (0.50 * numberOfIterations)) - 1)) / 2);
                out.println("Percentile 75th --------------");
                out.println("75th percentile number of write time: " + Duration.ofNanos(
                        (writeTimeMapValuesList.get((int) (0.75 * numberOfIterations)) + writeTimeMapValuesList.get(
                                ((int) (0.75 * numberOfIterations)) - 1)) / 2).toMillis());
                out.println("75th percentile number of avg wait time: " + Duration.ofNanos(
                        (avgWaitTimeMapList.get((int) (0.75 * numberOfIterations)) + avgWaitTimeMapList.get(
                                ((int) (0.75 * numberOfIterations)) - 1)) / 2).toMillis());
                out.println("75th percentile number of begin call time: " + Duration.ofNanos(
                        (beginCallMapValuesList.get((int) (0.75 * numberOfIterations)) + beginCallMapValuesList.get(
                                ((int) (0.75 * numberOfIterations)) - 1)) / 2).toMillis());
                out.println("75th percentile number of commit time: " + Duration.ofNanos(
                        (commitTimeMapValuesList.get((int) (0.75 * numberOfIterations)) + commitTimeMapValuesList.get(
                                ((int) (0.75 * numberOfIterations)) - 1)) / 2).toMillis());
                out.println("75th percentile number of transactions: " + (noOfTransactionsValuesList.get(
                        (int) (0.75 * numberOfIterations)) + noOfTransactionsValuesList.get(
                        ((int) (0.75 * numberOfIterations)) - 1)) / 2);
                out.println("Percentile 90th ---------------");
                out.println("90th percentile number of write time: " + Duration.ofNanos(
                        (writeTimeMapValuesList.get((int) (0.90 * numberOfIterations)) + writeTimeMapValuesList.get(
                                ((int) (0.90 * numberOfIterations)) - 1)) / 2).toMillis());
                out.println("90th percentile number of avg wait time: " + Duration.ofNanos(
                        (avgWaitTimeMapList.get((int) (0.90 * numberOfIterations)) + avgWaitTimeMapList.get(
                                ((int) (0.90 * numberOfIterations)) - 1)) / 2).toMillis());
                out.println("90th percentile number of begin call time: " + Duration.ofNanos(
                        (beginCallMapValuesList.get((int) (0.90 * numberOfIterations)) + beginCallMapValuesList.get(
                                ((int) (0.90 * numberOfIterations)) - 1)) / 2).toMillis());
                out.println("90th percentile number of commit time: " + Duration.ofNanos(
                        (commitTimeMapValuesList.get((int) (0.90 * numberOfIterations)) + commitTimeMapValuesList.get(
                                ((int) (0.90 * numberOfIterations)) - 1)) / 2).toMillis());
                out.println("90th percentile number of transactions: " + (noOfTransactionsValuesList.get(
                        (int) (0.90 * numberOfIterations)) + noOfTransactionsValuesList.get(
                        ((int) (0.90 * numberOfIterations)) - 1)) / 2);
            } else {
                out.println("50th percentile number of write time: " + Duration.ofNanos(
                        writeTimeMapValuesList.get((int) (0.50 * numberOfIterations))).toMillis());
                out.println("50th percentile number of avg wait time: " + Duration.ofNanos(
                        avgWaitTimeMapList.get((int) (0.50 * numberOfIterations))).toMillis());
                out.println("50th percentile number of begin call time: " + Duration.ofNanos(
                        beginCallMapValuesList.get((int) (0.50 * numberOfIterations))).toMillis());
                out.println("50th percentile number of commit time: " + Duration.ofNanos(
                        commitTimeMapValuesList.get((int) (0.50 * numberOfIterations))).toMillis());
                out.println("50th percentile number of transactions: " + noOfTransactionsValuesList.get(
                        (int) (0.50 * numberOfIterations)));
                out.println("Percentile 75th --------------");
                out.println("75th percentile number of write time: " + Duration.ofNanos(
                        writeTimeMapValuesList.get((int) (0.75 * numberOfIterations))).toMillis());
                out.println("75th percentile number of avg wait time: " + Duration.ofNanos(
                        avgWaitTimeMapList.get((int) (0.75 * numberOfIterations))).toMillis());
                out.println("75th percentile number of begin call time: " + Duration.ofNanos(
                        beginCallMapValuesList.get((int) (0.75 * numberOfIterations))).toMillis());
                out.println("75th percentile number of commit time: " + Duration.ofNanos(
                        commitTimeMapValuesList.get((int) (0.75 * numberOfIterations))).toMillis());
                out.println("75th percentile number of transactions: " + noOfTransactionsValuesList.get(
                        (int) (0.75 * numberOfIterations)));
                out.println("Percentile 90th ---------------");
                out.println("90th percentile number of write time: " + Duration.ofNanos(
                        writeTimeMapValuesList.get((int) (0.90 * numberOfIterations))).toMillis());
                out.println("90th percentile number of avg wait time: " + Duration.ofNanos(
                        avgWaitTimeMapList.get((int) (0.90 * numberOfIterations))).toMillis());
                out.println("90th percentile number of begin call time: " + Duration.ofNanos(
                        beginCallMapValuesList.get((int) (0.90 * numberOfIterations))).toMillis());
                out.println("90th percentile number of commit time: " + Duration.ofNanos(
                        commitTimeMapValuesList.get((int) (0.90 * numberOfIterations))).toMillis());
                out.println("90th percentile number of transactions: " + noOfTransactionsValuesList.get(
                        (int) (0.90 * numberOfIterations)));
            }
            out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        }
    }
}