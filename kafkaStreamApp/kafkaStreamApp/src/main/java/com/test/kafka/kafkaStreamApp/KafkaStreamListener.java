package com.test.kafka.kafkaStreamApp;

import java.util.function.Function;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaStreamListener {

    private static Logger logger = LogManager.getLogger(KafkaStreamListener.class);
    
    //bean for processing autonomous messages and pushing out avro formats
    //@SobyChako : I am able to push messages as String and receive as String but deserialization does not work for JSONObject
     @Bean
      public Function<KStream<String, JSONObject>, KStream<String, JSONObject>> autonomousProcessor() {
         System.out.println("start of stream processor%%%%%%%%%%%%%%%%%%%%%**************************");
         logger.info("inside processor");
         return kstream -> kstream.filter((key,value) -> {
         System.out.println(value.toString()); 
         return true;});
             }
}
