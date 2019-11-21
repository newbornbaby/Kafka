import conf.KafkaConfigure;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Test;
import org.springframework.kafka.core.ConsumerFactory;

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
        String topic = "FrmsDSQueue";
        KafkaConfigure kafkaConfigure = new KafkaConfigure();
        ConsumerFactory<String, String> consumerFactory =
                kafkaConfigure.defaultKafkaConsumerFactory(new Properties());
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        //订阅主题
        consumer.subscribe(Arrays.asList(topic));
        try {
            //拉取消息并消费
            while (isRunning.get()) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {

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
}
