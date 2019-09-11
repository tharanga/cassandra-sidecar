package org.apache.cassandra.sidecar.cdc.output;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.transform.Filter;
import org.apache.cassandra.utils.FBUtilities;


/**
 * Null output for Cassandra PartitionUpdates.
 */
public class ConsoleOutput implements Output
{

    private static final Logger logger = LoggerFactory.getLogger(ConsoleOutput.class);

    @Override
    public void emitPartition(PartitionUpdate partition) throws Exception
    {
        logger.info("Handling a partition with the column family : {}", partition.metadata().name);
        String pkStr = partition.metadata().partitionKeyType.getSerializer()
                .toCQLLiteral(partition.partitionKey().getKey());
        logger.info("> Partition Key : {}", pkStr);

        if (partition.staticRow().columns().size() > 0)
        {
            logger.info("> -- Static columns : {} ", partition.staticRow().toString(partition.metadata(), false));
        }
        Filter fl = new Filter(FBUtilities.nowInSeconds(), false);
        RowIterator ri = fl.applyToPartition(partition.unfilteredIterator());
        while (ri.hasNext())
        {
            Row r = ri.next();
            logger.info("> -- Row contents: {}", r.toString(partition.metadata(), false));
        }
    }
}