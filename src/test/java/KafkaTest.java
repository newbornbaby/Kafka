import conf.CustomSerialize;
import conf.KafkaConfigure;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.Test;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import pojo.User;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Description TODO <br>
 * @Author SpiderMao <br>
 * @Version 1.0 <br>
 * @CreateDate 2019/11/20 16:52 <br>
 * @See cn.com.bsfit.test <br>
 */
public class KafkaTest {

    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * @Description 测试类
     * @Author SpiderMao
     * @CreateDate 2019/11/20 18:19
     */
    @Test
    public void test() {
        String topic = "testB";
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerialize.class);
        KafkaConfigure kafkaConfigure = new KafkaConfigure();
        ConsumerFactory<Integer, User> consumerFactory =
                kafkaConfigure.customKafkaConsumerFactory(properties);
        Consumer<Integer, User> consumer = consumerFactory.createConsumer();
        //订阅主题
        consumer.subscribe(Arrays.asList(topic));
        try {
            //拉取消息并消费
            while (isRunning.get()) {

                ConsumerRecords<Integer, User> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<Integer, User> record : records) {

                    System.out.println("topic=" + record.topic() + ",partition=" + record.partition() + ",offset=" + record.offset());
                    System.out.println("key=" + record.key() + ",value=" + record.value());
                    //do something to processor record.
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    /**
     * @Description 测试类-kafka生产者
     * @Author SpiderMao
     * @CreateDate 2019/11/25 14:13
     */
    @Test
    public void test1() {
        String topic = "testA";
        KafkaConfigure kafkaConfigure = new KafkaConfigure();
        ProducerFactory<String, String> producerFactory =
                kafkaConfigure.defaultKafkaProducerFactory(new Properties());
        Producer<String, String> producer = producerFactory.createProducer();

        String time = String.valueOf(System.currentTimeMillis());

        ProducerRecord<String, String> record
                = new ProducerRecord<>(topic, time, time);

        /*try {
            // 无回调发送消息
            //producer.send(record);
            // 同步发送消息
            //producer.send(record).get();
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        while (true) {
            // 异步发送
            producer.send(record, new DemoProducerCallback());
        }
    }

    /**
     * @Description 自定义序列化器
     * @Author SpiderMao
     * @CreateDate 2019/11/25 16:32
     */
    @Test
    public void test2() {
        String topic = "testB";
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerialize.class);
        KafkaConfigure kafkaConfigure = new KafkaConfigure();
        ProducerFactory<Integer, User> producerFactory =
                kafkaConfigure.customKafkaProducerFactory(properties);
        Producer<Integer, User> producer = producerFactory.createProducer();

        //String time = String.valueOf(System.currentTimeMillis());

        ProducerRecord<Integer, User> record
                = new ProducerRecord<>(topic, 1, new User(1, "mao11"));

        try {
            // 同步发送消息
            producer.send(record).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class DemoProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                System.out.println("offset" + metadata.offset() + "timestamp"
                        + metadata.timestamp() + "topicPartition" + metadata.partition());
            }
        }
    }
}
