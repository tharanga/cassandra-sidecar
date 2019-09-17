package org.apache.cassandra.sidecar.cdc;

import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.config.Schema;
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

    SSTableDumper(Configuration conf, String keySpace, String columnFamily)
    {
        this.conf = conf;
        this.keySpace = keySpace;
        this.columnFamily = columnFamily;
        producer = OutputFactory.getOutput(conf);
    }

    public void dump()
    {
        // TODO: Flush data before starting the data dump
        //if (Schema.instance.getCFMetaData(keySpace, columnFamily) == null) {
        if (producer == null)
        {
            logger.error("Output producer is not properly initiated");
            return;
        }
        if (Schema.instance.getCFMetaData(keySpace, columnFamily) == null)
        {
            logger.error("Unknown keySpace/columnFamily {}.{}. No data to dump", keySpace, columnFamily);
            return;
        }

        ColumnFamilyStore cfs = null;
        String snapshotName = UUID.randomUUID().toString();

        try
        {
            // TODO: SSTables have index and data files. Index file has partition keys with offsets to the data file. We
            // can save a LOT of CPU cycles by not de-serializing PartitionUpdate objects. Instead we can read
            // byte offsets and send them directly to the Kafka.
            Keyspace ks = Keyspace.open(keySpace);
            // TODO: Don't use the column store. Adding and removing to/from the cf conflicts with the server.
            cfs = ks.getColumnFamilyStore(columnFamily);
            // TODO: Flush before dump.
            ColumnFamilyStore.loadNewSSTables(keySpace, columnFamily);
            Set<SSTableReader> ssTables = cfs.snapshotWithoutFlush(snapshotName, null,  true);

            for (SSTableReader reader : ssTables)
            {
                if (reader != null)
                {
                    UnfilteredPartitionIterator ufp = reader.getScanner();
                    while (ufp.hasNext())
                    {
                        PartitionUpdate partition = PartitionUpdate.fromIterator(ufp.next(),
                                ColumnFilter.all(ufp.metadata()));
                        producer.emitChange(new Change(PayloadType.PARTITION_UPDATE, MessagingService.current_version,
                                Change.REFRESH_EVENT, partition));
                    }
                    ufp.close();
                }
            }
        }
        catch (Exception ex)
        {
            logger.error("Couldn't open/read the sstable :  {} : {}", keySpace, columnFamily);
        }
        finally
        {
            if (cfs != null && cfs.snapshotExists(snapshotName))
            {
                cfs.clearSnapshot(snapshotName);
            }
        }
    }
}
