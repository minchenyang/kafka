package com.min.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Map;

/**
 * @program: kafka
 * @description: 自定义分区器
 * @author: mcy
 * @create: 2018-08-06 19:06
 **/
public class CustomPartition implements Partitioner {

    @Override
    public int partition(final String topic, final Object key, final byte[] keyBytes, final Object value, final byte[] valueBytes1, final Cluster cluster) {

        //获取topic 相应分区的数量
        Integer integer = cluster.partitionCountForTopic(topic);

        if(null== keyBytes || !(key instanceof String)){
            throw new InvalidRecordException("KAFKA参数必须存在且为String类型");
        }

        if( integer == 1 ){
            return 0;
        }

        if("name".equals(key)){
            return integer - 1;
        }
        return Math.abs(Utils.murmur2(keyBytes));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(final Map<String, ?> map) {

    }
}
