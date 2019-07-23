package org.apache.cassandra.sidecar.cdc;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.StringSerializer;


/**
 * Reads data from SSTables
 */
public class SSTableDumper
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableDumper.class);
    private Configuration conf;
    private String keySpace;
    private String columnFamily;
    private String topic;
    private  Producer<String, ByteBuffer> producer;

    SSTableDumper(Configuration conf)
    {
        this.conf = conf;
        this.keySpace = conf.getKeySpace();
        this.columnFamily = conf.getColumnFamily();
        this.topic = conf.getKafkaTopic();
        final Map<String, Object> producerConfig = Maps.newHashMap();
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class.getName());
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getKafkaServer());
        producer = new KafkaProducer<>(producerConfig);
    }

    public void dump()
    {
        //if (Schema.instance.getCFMetaData(keySpace, columnFamily) == null) {
        if (Schema.instance.getTableMetadata(keySpace, columnFamily) == null)
        {
            logger.error("Unknown keySpace/columnFamily {}.{}. No data to dump", keySpace, columnFamily);
            return;
        }

        Keyspace ks = Keyspace.openWithoutSSTables(keySpace);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(columnFamily);
        Directories.SSTableLister lister = cfs.getDirectories().sstableLister(null).skipTemporary(true);
        for (Map.Entry<Descriptor, Set<Component>> sstable : lister.list().entrySet())
        {
            if (sstable.getKey() != null)
            {
                try
                {
                    SSTableReader reader = SSTableReader.open(sstable.getKey());
                    Keyspace.open(keySpace).getColumnFamilyStore(columnFamily).addSSTable(reader);
                    if (reader != null)
                    {

                        UnfilteredPartitionIterator ufp = reader.getScanner();
                        while (ufp.hasNext())
                        {
                            PartitionUpdate partition = PartitionUpdate.fromIterator(ufp.next(),
                                    ColumnFilter.all(ufp.metadata()));
                            String partitionKey = partition.metadata().partitionKeyType.getSerializer()
                                    .toCQLLiteral(partition.partitionKey().getKey());
                            //String partitionKey = partition.metadata().getKeyValidator()
                            // .getString(partition.partitionKey().getKey());
                            logger.debug("Dumping a partition update with the key : {}",  partitionKey);
                            ProducerRecord<String, ByteBuffer> record = new ProducerRecord<>(this.topic, partitionKey,
                                    PartitionUpdate.toBytes(partition, 1));
                            producer.send(record);
                        }
                        ufp.close();
                    }
                }
                catch (Throwable t)
                {
                    logger.error("Couldn't open/read the sstable :  {}", sstable.getKey().filenameFor(Component.DATA));
                }
            }
        }


    }
}
