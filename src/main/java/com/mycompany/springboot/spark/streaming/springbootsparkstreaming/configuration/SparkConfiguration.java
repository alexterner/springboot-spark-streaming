package com.mycompany.springboot.spark.streaming.springbootsparkstreaming.configuration;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;


@Configuration
public class SparkConfiguration {

    @Autowired
    private Environment env;

    @Value("${app.name:Springboot-Spark-Streaming}")
    private String appName;

    @Value("${spark.home}")
    private String sparkHome;

    @Value("${master.uri:local}")
    private String masterUri;

    @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                //.setSparkHome(sparkHome)
                .setMaster(masterUri);

        return sparkConf;
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext( sparkConf() );
    }

    @Bean
    public SparkSession sparkSession() {
        return SparkSession
                .builder()
                .sparkContext(javaSparkContext().sc())
                .appName( appName )
                .getOrCreate();
    }

    @Bean
    public JavaStreamingContext javaStreamingContext(){
        return new JavaStreamingContext( sparkConf(), Duration.apply(2) );
    }



    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

}
