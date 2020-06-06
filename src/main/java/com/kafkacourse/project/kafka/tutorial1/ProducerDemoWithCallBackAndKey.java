package com.kafkacourse.project.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class ProducerDemoWithCallBackAndKey {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBackAndKey.class);

        String bootStrapServers = "127.0.0.1:9092";

        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for(int i=0; i<10; i++) {

            String topic = "first_topic";
            String value = "Hello WorldKey";
            String key = "id_" + Integer.toString(i);

            logger.info("Key:" + key);
            //Create Producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            //send data asynchronously
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("Received metadata. " + "\n" + "Topic:" + metadata.topic()
                                + "\n" + "Partition:" + metadata.partition()
                                + "\n" + "Offset:" + metadata.offset());
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            }).get();
        }
        //flush the data
        producer.flush();

        //flush and close
        producer.close();
    }
}
