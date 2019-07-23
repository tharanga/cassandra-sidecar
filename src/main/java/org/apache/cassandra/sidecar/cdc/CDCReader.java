package org.apache.cassandra.sidecar.cdc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.StringSerializer;


/**
 * Reads mutations from the Cassandra commit logs.
 */
public class CDCReader implements CommitLogReadHandler
{
    private Configuration conf;
    private static final Logger logger = LoggerFactory.getLogger(CDCReader.class);
    private  Producer<String, ByteBuffer> producer = null;
    private String topic;

    public CDCReader(Configuration conf)
    {
        this.conf = conf;
        this.topic = conf.getKafkaTopic();
        final Map<String, Object> producerConfig = Maps.newHashMap();
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class.getName());
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getKafkaServer());
        producer = new KafkaProducer<>(producerConfig);
    }

    @Override
    public boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException
    {
        return false;
    }

    @Override
    public void handleUnrecoverableError(CommitLogReadException exception) throws IOException
    {
    }

    @Override
    public void handleMutation(Mutation mutation, int size, int entryLocation, CommitLogDescriptor desc)
    {
        if (mutation == null || !mutation.getKeyspaceName().equals(conf.getKeySpace()))
        {
            return;
        }
        logger.debug("Started handling a mutation of the keyspace : {}", mutation.getKeyspaceName());

        for (PartitionUpdate partitionUpdate : mutation.getPartitionUpdates())
        {
            handlePartitionUpdate(partitionUpdate);
        }
    }

    private  void handlePartitionUpdate(PartitionUpdate partition)
    {
        //if (producer == null || partition == null || partition.metadata().cfName.equals(config.getColumnFamily())){
        if (producer == null || partition == null || !partition.metadata().name.equals(conf.getColumnFamily()))
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
            ProducerRecord<String, ByteBuffer> record = new ProducerRecord<>(this.topic, partitionKey,
                    PartitionUpdate.toBytes(partition, 1));
            this.producer.send(record);
        }
        catch (Exception ex)
        {
            logger.error("Error sending a message to the Kafka : {}" + ex.getMessage());
        }
    }
}
