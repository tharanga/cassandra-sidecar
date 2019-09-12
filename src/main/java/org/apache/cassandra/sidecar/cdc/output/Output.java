package org.apache.cassandra.sidecar.cdc.output;

import org.apache.cassandra.sidecar.cdc.Change;

/**
 * Interface for emitting Cassandra PartitionUpdates
 */
public interface Output
{
    void emitChange(Change change) throws Exception;
}
