package org.apache.cassandra.sidecar.cdc.output;

import org.apache.cassandra.db.partitions.PartitionUpdate;

/**
 * Interface for emitting Cassandra PartitionUpdates
 */
public interface Output
{
    void emitPartition(PartitionUpdate partition) throws Exception;
}
