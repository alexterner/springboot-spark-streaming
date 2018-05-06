package com.mycompany.springboot.spark.streaming.springbootsparkstreaming.runner;



import com.mycompany.springboot.spark.streaming.springbootsparkstreaming.post.processor.AutowiredLog;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;


@Component
public class ApplicationRunner implements CommandLineRunner{

    private static final Pattern SPACE = Pattern.compile(" ");

    @AutowiredLog
    private Logger logger;

    @Autowired
    private Environment env;

    @Autowired
    private JavaStreamingContext jssc;

    @Value("${streaming.host:localhost}")
    private String streamingHost;

    @Value("${streaming.port}")
    private String streamingPort;

    @Value(("${streaming.isStateFull:false}"))
    private String isStateFull;

    @Override
    public void run(String... args) throws Exception {

        boolean isStateFullEnv = Boolean.valueOf(isStateFull);

        if( !isStateFullEnv){
            new StateLessExample().invoke();
        } else{
            new StateFullExample().invoke();
        }


    }


    private class StateLessExample {

        public void invoke() throws InterruptedException {

            logger.info("=> Start state less example");

            logger.info("Start Spark Streaming to listen on host :" + streamingHost + ", port : " + streamingPort);


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


    private class StateFullExample {

        public void invoke() throws InterruptedException {

            logger.info("=> Start state full example");

            logger.info("Start Spark Streaming to listen on host :" + streamingHost + ", port : " + streamingPort);

            // Initial state RDD input to mapWithState
            @SuppressWarnings("unchecked")
            List<Tuple2<String, Integer>> initValues = Arrays.asList(
                    new Tuple2<>("hello", 1),
                    new Tuple2<>("world", 1));

            JavaPairRDD<String, Integer> initialRDD = jssc.sparkContext().parallelizePairs(initValues);



            // Create a DStream that will connect to hostname:port, like localhost:12345
            JavaReceiverInputDStream<String> lines = jssc.socketTextStream(streamingHost, Integer.valueOf(streamingPort), StorageLevels.MEMORY_ONLY);

            // Split each line into words
            JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

            // Count each word in each batch
            JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));

            // Update the cumulative count function
            Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                    (word, one, state) -> {
                        int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
                        Tuple2<String, Integer> output = new Tuple2<>(word, sum);
                        state.update(sum);
                        return output;
                    };

            // DStream made of get cumulative counts that get updated in every batch
            JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
                    pairs.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));


            // Print the first ten elements of each RDD generated in this DStream to the console
            pairs.print();


            jssc.start();              // Start the computation
            jssc.awaitTermination();   // Wait for the computation to terminate
        }
    }
}
