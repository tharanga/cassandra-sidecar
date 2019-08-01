package org.apache.cassandra.sidecar.cdc;

import java.util.Map;
import java.util.Set;

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
import org.apache.cassandra.sidecar.cdc.output.Output;
import org.apache.cassandra.sidecar.cdc.output.OutputFactory;




/**
 * Reads data from SSTables
 */
public class SSTableDumper
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableDumper.class);
    private Configuration conf;
    private String keySpace;
    private String columnFamily;
    private Output producer;

    SSTableDumper(Configuration conf)
    {
        this.conf = conf;
        this.keySpace = conf.getKeySpace();
        this.columnFamily = conf.getColumnFamily();
        producer = OutputFactory.getOutput(conf);
    }

    public void dump()
    {
        //if (Schema.instance.getCFMetaData(keySpace, columnFamily) == null) {
        if (producer == null)
        {
            logger.error("Output producer is not properly initiated");
            return;
        }
        if (Schema.instance.getTableMetadata(keySpace, columnFamily) == null)
        {
            logger.error("Unknown keySpace/columnFamily {}.{}. No data to dump", keySpace, columnFamily);
            return;
        }
        // TODO: SSTables have index and data files. Index file has partition keys with offsets to the data file. We
        // can save a LOT of CPU cycles by not de-serializing PartitionUpdate objects. Instead we can read byte offsets
        // and send them directly to the Kafka.
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
                            producer.emitPartition(partition);
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
