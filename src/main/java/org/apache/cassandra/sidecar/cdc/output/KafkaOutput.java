package org.apache.cassandra.sidecar.cdc.output;


import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.collect.Maps;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.StringSerializer;


/**
 * Emits Cassandra PartitionUpdates to a Kafka queue.
 */
public class KafkaOutput implements Output
{

    private static final Logger logger = LoggerFactory.getLogger(KafkaOutput.class);
    private Producer<String, ByteBuffer> producer;
    private Configuration conf;
    Future<RecordMetadata> future = null;

    KafkaOutput(Configuration conf)
    {
        this.conf = conf;
        final Map<String, Object> producerConfig = Maps.newHashMap();
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class.getName());
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getKafkaServer());
        producer = new KafkaProducer<>(producerConfig);
    }

    @Override
    public void emitPartition(PartitionUpdate partition) throws Exception
    {
        //if (producer == null || partition == null || partition.metadata().cfName.equals(config.getColumnFamily())){
        if (producer == null)
        {
            throw new Exception("Kafka output is not properly configured");
        }

        if (partition == null)
        {
            return;
        }

        logger.debug("Started handling a partition with the column family : {}", partition.metadata().name);
        try
        {
            //String partitionKey = partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey());
            String partitionKey = partition.metadata().partitionKeyType.getSerializer()
                    .toCQLLiteral(partition.partitionKey().getKey());
            logger.debug("Producing a partition update with the key : {}",  partitionKey);
            ProducerRecord<String, ByteBuffer> record = new ProducerRecord<>(conf.getKafkaTopic(), partitionKey,
                    PartitionUpdate.toBytes(partition, 1));
            if (future != null)
            {
                future.get();
            }
            future = this.producer.send(record);
        }
        catch (Exception ex)
        {
            logger.error("Error sending a message to the Kafka : {}" + ex.getMessage());
            throw ex;
        }
    }
}
