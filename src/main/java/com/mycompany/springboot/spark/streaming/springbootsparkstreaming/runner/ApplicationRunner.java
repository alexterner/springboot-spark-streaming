package com.mycompany.springboot.spark.streaming.springbootsparkstreaming.runner;


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

@Component
public class ApplicationRunner implements CommandLineRunner{

    private static final Logger logger = LoggerFactory.getLogger(ApplicationRunner.class);

    private static final Pattern SPACE = Pattern.compile(" ");

    @Autowired
    private Environment env;

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private JavaStreamingContext jssc;

    @Autowired
    private JavaSparkContext sc;

    @Value("${streaming.host:localhost}")
    private String streamingHost;

    @Value("${streaming.port}")
    private String streamingPort;

    @Override
    public void run(String... args) throws Exception {

        // Create a DStream that will connect to hostname:port, like localhost:12345
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(streamingHost, Integer.valueOf(streamingPort));

        // Split each line into words
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate

    }


}
