package conf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Description kafka <br>
 * @Author SpiderMao <br>
 * @Version 1.0 <br>
 * @CreateDate 2019/11/21 14:13 <br>
 * @See conf <br>
 */
public class KafkaConfigure {

    /**
     * broker集群地址
     */
    private String brokerAddress = "192.168.3.111:9092";

    /**
     * zookeeper地址
     */
    private String zookeeperConnect = "192.168.3.111:2181";


    //==================producer配置================================\\
    // 数据备份的可用性, acks=all： 这意味着leader需要等待所有备份都成功写入日志
    // 需要server接收到数据之后发出的确认接收的信号，此项配置就是指procuder需要多少个这样的确认信号
    private String acks = "all";

    // 默认的批量处理消息字节数，单位字节
    private String batchSize = "16384";

    // 最大请求字节数，单位字节
    private String maxRequestSize = "33554432";

    // producer可以用来缓存数据的内存大小，单位字节
    private String bufferMemory = "33554432";

    // eventHandler定期的刷新metadata的间隔,-1只有在发送失败时才会重新刷新
    private String metadataRefresh = "-1";

    private String retries = "0";

    //强制元数据时间间隔
    private String metadataMaxAge = "120000";

    private String maxBlockMS = "6000";

    /**
     * 压缩方式
     */
    private String compressionType;
    //=================end producer================================\\


    //=================consumer配置===============================\\

    private String groupId = "frms";
    // ConsumerConfig.AUTO_OFFSET_RESET_DOC
    private String offsetReset = "earliest";

    // 为true时consumer所fetch的消息的offset将会自动的同步到zookeeper
    private String autoCommitFlag = "true";

    // consumer向zookeeper提交offset的频率，单位毫秒
    private String autoCommitInterval = "1000";

    // 会话的超时限制，单位毫秒
    private String sessionTimeout = "15000";

    // 消费者能读取的最大消息,单位字节。这个值应该大于或等于message.max.bytes;默认值 50*1024*1024
    private String fetch_max_bytes = "52428800";

    private String fetch_max_wait_ms = "6000";

    //kafka 平衡的时间间隔
    private String max_poll_interval_ms = "120000";


    /**
     * @Description 消费者工厂
     * @Author SpiderMao
     * @CreateDate 2019/11/21 14:49
     * @Param Properties
     * @Return ConsumerFactory
     */
    public ConsumerFactory<String, String> defaultKafkaConsumerFactory(Properties consumerProperties) {
        Map<String, Object> propsMap = new HashMap<String, Object>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                this.brokerAddress);
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommitFlag);
        propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                autoCommitInterval);
        propsMap.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        propsMap.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetch_max_bytes);
        propsMap.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
                fetch_max_wait_ms);
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        propsMap.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, max_poll_interval_ms);
        resetProperties(consumerProperties, propsMap);
        if (propsMap.containsKey("security.krb5.conf"))
            System.setProperty("java.security.krb5.conf", String.valueOf(propsMap.get("security.krb5.conf")));
        if (propsMap.containsKey("security.login.conf"))
            System.setProperty("java.security.auth.login.config", String.valueOf(propsMap.get("security.login.conf")));
        return new DefaultKafkaConsumerFactory<>(propsMap);
    }


    public ProducerFactory<String, String> defaultKafkaProducerFactory(Properties producerProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMS);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, metadataMaxAge);
        //压缩配置
        if (compressionType != null && !"".equals(compressionType)) {
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        }
        resetProperties(producerProperties, props);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * 对x_y_z这样的key转换为x.y.z
     *
     * @param properties
     * @param propsMap
     * @return
     */
    private void resetProperties(Properties properties,
                                 Map<String, Object> propsMap) {
        for (Map.Entry<Object, Object> property : properties.entrySet()) {
            propsMap.put(((String) property.getKey()).replace("_", "."),
                    property.getValue());
        }
    }
}
