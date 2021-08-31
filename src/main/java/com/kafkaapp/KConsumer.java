package com.kafkaapp;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka POC - On Local Kafka Server
 * ==============
 * Kafka Consumer
 * ==============
 */
public class KConsumer {
    public static void main( String[] args ){

        // Logger
        final Logger logger = LoggerFactory.getLogger(KProducer.class);

        // DEFAULT PRODUCERS CONFIG
        String KAFKA_BOOTSTRAPSERVER = "127.0.0.1:9092";
        String KAFKA_DESERIALIZERNAME = StringDeserializer.class.getName();
        String KAFKA_TOPIC = "first_topic";
        String GROUP_ID = "k_consumer";
        
        logger.info( "===== KAFKA CONSUMER DEMO ON LOCAL MACHINE =====" );

        /**
         * Creating Kafka Consumer
         * 1. Create Consumer Properties
         * 2. Create a Consumer
         * 3. Subscribe to a specific topic.
         * 4. Keep Polling for Data
         */

        // 1. Creating Kafka Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAPSERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZERNAME);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZERNAME);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        // EARLIEST (READ FROM BEGINNING), LATEST (READ FROM THE FIRST LATEST RECORD), NONE (READ FROM LAST COMMITED OFFSET - NOTE: THIS WILL THROW AN ERROR, IF NO COMMITS FOUND IN THE OFFSET FOR THIS CONSUMER)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 2. Creating a Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 3. Subscribe to a Topic(S) - We can also subscribe to multiple topics by providing array of topics.
        consumer.subscribe(Collections.singleton(KAFKA_TOPIC));

        // 4. Poll for Data every 100ms.
        while (true) {
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));

            // Iterating from all the records.
            for(ConsumerRecord<String, String> record: records){
                logger.info(
                    "\n" +
                    "===== MESSAGE RECIEVED =====" + "\n" +
                    "Message  :" + record.value() + "\n" +
                    "Key      :" + record.key() + "\n" +
                    "Partiton :" + record.partition() + "\n" +
                    "Offset   :" + record.offset() + "\n" + 
                    "============================" +
                    "\n"
                );
            }
        }
    }
}
