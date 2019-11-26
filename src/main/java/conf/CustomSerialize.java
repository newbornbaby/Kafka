package conf;

import org.apache.kafka.common.serialization.Serializer;
import org.springframework.util.StringUtils;
import pojo.User;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @Description 自定义序列化器 <br>
 * @Author SpiderMao <br>
 * @Version 1.0 <br>
 * @CreateDate 2019/11/25 16:07 <br>
 * @See conf <br>
 */
public class CustomSerialize implements Serializer {

    @Override
    public void configure(Map configs, boolean isKey) {
        // 不做任何配置
    }

    /**
     * @Description user被序列化 组成是：
     * 表示user-id的四字节整数
     * 表示user-name长度的四字节整数
     * 表示user-name的n字节
     * @Author SpiderMao
     * @CreateDate 2019/11/25 16:25
     * @Param topic data
     * @Return byte[]
     */
    @Override
    public byte[] serialize(String topic, Object data) {
        byte[] name = null;
        int nameSize = 0;
        try {
            if (data == null) {
                return null;
            }
            User user = (User) data;
            if (!StringUtils.isEmpty(user.getName())) {
                name = user.getName().getBytes("UTF-8");
                nameSize = name.length;
            } else {
                name = new byte[0];
            }
            ByteBuffer bf = ByteBuffer.allocate(4 + 4 + nameSize);
            bf.putInt(user.getId());
            bf.putInt(nameSize);
            bf.put(name);
            return bf.array();
        } catch (Exception e) {
            e.printStackTrace();
        }
        ByteBuffer bf = ByteBuffer.allocate(100);
        return new byte[0];
    }

    @Override
    public void close() {
        // 不需要关闭任何东西
    }
}
