package com.kafkaapp;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka POC - On Local Kafka Server
 */
public class App 
{
    public static void main( String[] args )
    {
        // Logger
        final Logger logger = LoggerFactory.getLogger(App.class);

        // DEFAULT PRODUCERS CONFIG
        String KAFKA_BOOTSTRAPSERVER = "127.0.0.1:9092";
        String KAFKA_SERIALIZERNAME = StringSerializer.class.getName();
        
        logger.info( "===== KAFKA DEMO ON LOCAL MACHINE =====" );

        /**
         * Creating Kafka Producer
         * 1. Create Producer Properties
         * 2. Create a Producer
         * 3. Send Message/Data to the topic
         */

        // 1. Creating Kafka Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAPSERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZERNAME);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZERNAME);

        // 2. Creating Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        
        // 3. Send Message/Data to the topic

        // Creating a record with a message, to be send on a topic.
        ProducerRecord<String, String> record = new ProducerRecord<String,String>("first_topic", "Hello Kafka."); 
        // Asynchronous - Sending Data to a topic.
        producer.send(record, new Callback(){
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                // Callback when data is sent to the Kafka Server
                if(exception != null){
                    logger.error("Error while Producing Data", exception);
                }else{
                    logger.info(
                        "\n" +
                        "=== MetaData === \n" + 
                        "Topic           : " + metadata.topic() + "\n" +
                        "Topic Partition : " + metadata.partition() + "\n" +
                        "Offset          : " + metadata.offset() + "\n" +
                        "Timestamp       : " + metadata.timestamp() + "\n"
                    );
                }
            }
        });
        // Waiting for Transanction to be complete, if we don't wait then the data won't be send, since the execution will be completed before sending.
        producer.flush();
        // Flush and close producer
        producer.close();
    }
}
