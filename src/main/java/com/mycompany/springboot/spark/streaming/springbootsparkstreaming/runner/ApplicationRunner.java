package com.mycompany.springboot.spark.streaming.springbootsparkstreaming.runner;


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

@Component
public class ApplicationRunner implements CommandLineRunner{

    private static final Logger logger = LoggerFactory.getLogger(ApplicationRunner.class);

    private static final Pattern SPACE = Pattern.compile(" ");

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private JavaStreamingContext streamContext;

    @Autowired
    private JavaSparkContext sc;

    @Override
    public void run(String... args) throws Exception {

    }


}
