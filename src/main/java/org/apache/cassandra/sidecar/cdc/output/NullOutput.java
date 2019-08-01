package org.apache.cassandra.sidecar.cdc.output;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.partitions.PartitionUpdate;

/**
 * Null output for Cassandra PartitionUpdates.
 */
public class NullOutput implements Output
{

    private static final Logger logger = LoggerFactory.getLogger(NullOutput.class);

    @Override
    public void emitPartition(PartitionUpdate partition) throws Exception
    {
        logger.info("Handling a partition with the column family : {}", partition.metadata().name);
    }
}
