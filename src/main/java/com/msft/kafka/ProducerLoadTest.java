package com.msft.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.tools.ThroughputThrottler;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class ProducerLoadTest implements Runnable {



  private static byte[] message;

  private static long numRecords;
  private static int throughput;
  private static String topicName;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private static Properties properties;

  public ProducerLoadTest(Properties props) {
    properties = props;

  }

  public static long getNumRecords() {
    return numRecords;
  }

  public static void setNumRecords(long numRecords) {
    ProducerLoadTest.numRecords = numRecords;
  }

  public static int getThroughput() {
    return throughput;
  }

  public static void setThroughput(int throughput) {
    ProducerLoadTest.throughput = throughput;
  }


  public static byte[] getMessage() {
    return message;
  }

  public static void setMessage(byte[] message) {
    ProducerLoadTest.message = message;
  }

  public static String getTopicName() {
    return topicName;
  }

  public  static void setTopicName(String topicName) {
    ProducerLoadTest.topicName = topicName;
  }


  public void run() {
    KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(properties);

    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, message);
    Stats stats = new Stats(numRecords, 5000);
    long startMs = System.currentTimeMillis();

    ThroughputThrottler throttler = new ThroughputThrottler(throughput, startMs);

    for (int i = 0; i < numRecords; i++) {
      long sendStartMs = System.currentTimeMillis();
      Callback cb = stats.nextCompletion(sendStartMs, message.length, stats);
      producer.send(record, cb);
      if (throttler.shouldThrottle(i, sendStartMs)) {
        throttler.throttle();
      }
    }

    producer.close();
    stats.printTotal(Thread.currentThread().getName() + Thread.currentThread
        ().getId());
  }

  public static void main(String[] args) throws Exception {
    ArgumentParser parser = argParser();
    try {
      Namespace res = parser.parseArgs(args);
            /* parse args */
      String topicName = res.getString("topic");
      long numRecords = res.getLong("numRecords");
      int recordSize = res.getInt("recordSize");
      int throughput = res.getInt("throughput");
      int threads = res.getInt("threads");
      List<String> producerProps = res.getList("producerConfig");

      ProducerLoadTest.setNumRecords(numRecords);
      ProducerLoadTest.setThroughput(throughput);
      ProducerLoadTest.setTopicName(topicName);

      Properties props = new Properties();

      if (producerProps != null)
        for (String prop : producerProps) {
          String[] pieces = prop.split("=");
          if (pieces.length != 2)
            throw new IllegalArgumentException("Invalid property: " + prop);
          props.put(pieces[0], pieces[1]);
        }

      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

      // byte[] payload = Files.readAllBytes(Paths.get("sample" + ".txt"));
      //  ProducerLoadTest.setMessage(payload);

      byte[] payload = new byte[recordSize];
      Random random = new Random(System.currentTimeMillis());
      for (int i = 0; i < payload.length; ++i)
        payload[i] = (byte) (random.nextInt(26) + 65);
      ProducerLoadTest.setMessage(payload);

      for (int i = 0; i < threads; i++) {
        ProducerLoadTest producerLoadPerformance = new
            ProducerLoadTest(props);
        (new Thread(new ProducerLoadTest(props))).start();
      }

    } catch (ArgumentParserException e) {
      if (args.length == 0) {
        parser.printHelp();
        System.exit(0);
      } else {
        parser.handleError(e);
        System.exit(1);
      }
    }
  }
  /** Get the command-line argument parser. */
  private static ArgumentParser argParser() {
    ArgumentParser parser = ArgumentParsers
        .newArgumentParser("producer-performance")
        .defaultHelp(true)
        .description("This tool is used to verify the producer performance.");
    parser.addArgument("--topic")
          .action(store())
          .required(true)
          .type(String.class)
          .metavar("TOPIC")
          .help("produce messages to this topic");
    parser.addArgument("--num-records")
          .action(store())
          .required(true)
          .type(Long.class)
          .metavar("NUM-RECORDS")
          .dest("numRecords")
          .help("number of messages to produce");
    parser.addArgument("--record-size")
          .action(store())
          .required(true)
          .type(Integer.class)
          .metavar("RECORD-SIZE")
          .dest("recordSize")
          .help("message size in bytes");
    parser.addArgument("--threads")
          .action(store())
          .required(true)
          .type(Integer.class)
          .metavar("THREADS")
          .dest("threads")
          .help("number of threads");
    parser.addArgument("--throughput")
          .action(store())
          .required(true)
          .type(Integer.class)
          .metavar("THROUGHPUT")
          .help("throttle maximum message throughput to *approximately* THROUGHPUT messages/sec");
    parser.addArgument("--producer-props")
          .nargs("+")
          .required(true)
          .metavar("PROP-NAME=PROP-VALUE")
          .type(String.class)
          .dest("producerConfig")
          .help("kafka producer related configuaration properties like bootstrap.servers,client.id etc..");
    return parser;
  }
  private class Stats {
    private long start;
    private long windowStart;
    private int[] latencies;
    private int sampling;
    private int iteration;
    private int index;
    private long count;
    private long bytes;
    private int maxLatency;
    private long totalLatency;
    private long windowCount;
    private int windowMaxLatency;
    private long windowTotalLatency;
    private long windowBytes;
    private long reportingInterval;
    public Stats(long numRecords, int reportingInterval) {
      this.start = System.currentTimeMillis();
      this.windowStart = System.currentTimeMillis();
      this.index = 0;
      this.iteration = 0;
      this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
      this.latencies = new int[(int) (numRecords / this.sampling) + 1];
      this.index = 0;
      this.maxLatency = 0;
      this.totalLatency = 0;
      this.windowCount = 0;
      this.windowMaxLatency = 0;
      this.windowTotalLatency = 0;
      this.windowBytes = 0;
      this.totalLatency = 0;
      this.reportingInterval = reportingInterval;
    }
    public void record(int iter, int latency, int bytes, long time) {
      this.count++;
      this.bytes += bytes;
      this.totalLatency += latency;
      this.maxLatency = Math.max(this.maxLatency, latency);
      this.windowCount++;
      this.windowBytes += bytes;
      this.windowTotalLatency += latency;
      this.windowMaxLatency = Math.max(windowMaxLatency, latency);
      if (iter % this.sampling == 0) {
        this.latencies[index] = latency;
        this.index++;
      }
            /* maybe report the recent perf */
      if (time - windowStart >= reportingInterval) {
        printWindow();
        newWindow();
      }
    }
    public Callback nextCompletion(long start, int bytes, Stats stats) {
      Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
      this.iteration++;
      return cb;
    }
    public void printWindow() {
      long ellapsed = System.currentTimeMillis() - windowStart;
      double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
      double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
      System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f max latency.\n",
                        windowCount,
                        recsPerSec,
                        mbPerSec,
                        windowTotalLatency / (double) windowCount,
                        (double) windowMaxLatency);
    }
    public void newWindow() {
      this.windowStart = System.currentTimeMillis();
      this.windowCount = 0;
      this.windowMaxLatency = 0;
      this.windowTotalLatency = 0;
      this.windowBytes = 0;
    }
    public void printTotal(String threadName) {
      long elapsed = System.currentTimeMillis() - start;
      double recsPerSec = 1000.0 * count / (double) elapsed;
      double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
      int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
      System.out.printf(" %s, %d records sent, %f records/sec (%.2f " +
                        "MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
                        threadName,
                        count,
                        recsPerSec,
                        mbPerSec,
                        totalLatency / (double) count,
                        (double) maxLatency,
                        percs[0],
                        percs[1],
                        percs[2],
                        percs[3]);
    }
    private  int[] percentiles(int[] latencies, int count, double... percentiles) {
      int size = Math.min(count, latencies.length);
      Arrays.sort(latencies, 0, size);
      int[] values = new int[percentiles.length];
      for (int i = 0; i < percentiles.length; i++) {
        int index = (int) (percentiles[i] * size);
        values[i] = latencies[index];
      }
      return values;
    }
  }
  private  final class PerfCallback implements Callback {
    private final long start;
    private final int iteration;
    private final int bytes;
    private final Stats stats;
    public PerfCallback(int iter, long start, int bytes, Stats stats) {
      this.start = start;
      this.stats = stats;
      this.iteration = iter;
      this.bytes = bytes;
    }
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      long now = System.currentTimeMillis();
      int latency = (int) (now - start);
      this.stats.record(iteration, latency, bytes, now);
      if (exception != null)
        exception.printStackTrace();
    }
  }
}