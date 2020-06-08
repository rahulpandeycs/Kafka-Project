package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
      new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread(){

    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        Runnable myConsumerThread = new ConsumerThread(
                bootStrapServers,
                groupId,
                topic,
                latch); 

        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->
        {
            logger.info("Caught shutdown hook");
            ((ConsumerThread)(myConsumerThread)).shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application interrupted" + e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable{

        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer;
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        public ConsumerThread(String bootStrapServers, String groupId, String topic, CountDownLatch latch){
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //Create consumer
            consumer = new KafkaConsumer<String, String>(properties);
            //Subscribe consumer to topic
            consumer.subscribe(Arrays.asList(topic));

        }
        @Override
        public void run() {
           try{
            //poll for new data
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for(ConsumerRecord<String, String> record: records){
                    logger.info("Key" + record.key() );
                }
            }
           }catch (WakeupException exception){
                logger.info("Received shutdown signal!");
           }finally {
               consumer.close();
               latch.countDown();
           }
        }

        public void shutdown(){
             consumer.wakeup();
        }
    }
}
