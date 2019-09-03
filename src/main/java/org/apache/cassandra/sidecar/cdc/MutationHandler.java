package org.apache.cassandra.sidecar.cdc;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.cassandra.sidecar.cdc.output.Output;
import org.apache.cassandra.sidecar.cdc.output.OutputFactory;

/**
 * Implements Cassandra CommitLogReadHandler, dandles mutations read from Cassandra commit logs.
 */
public class MutationHandler implements CommitLogReadHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MutationHandler.class);
    private Configuration conf;
    private Output output;
    Future<Integer> mutationFuture = null;
    private ExecutorService executor;

    public MutationHandler(Configuration conf)
    {
        this.conf = conf;
        output = OutputFactory.getOutput(conf);
        executor = Executors.newSingleThreadExecutor();
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

        if (output == null)
        {
            logger.error("Output is not initialized");
        }

        logger.debug("Started handling a mutation of the keyspace : {} at offset {}", mutation.getKeyspaceName(),
                entryLocation);

        // Pipeline Mutation reading and de-serializing with sending to the output.
        //TODO: Multiple threads can process Mutations in parallel; hence use a thread pool. Be careful to design
        // bookmarks and commit log deletion to work with multiple threads.
        if (mutationFuture != null)
        {
            try
            {
                Integer completedLocation = mutationFuture.get();
                //TODO: Save the last bookmark, so CDC reader can resume instead of restarting when
                // the process restarts
                logger.debug("Completed sending data at offset {}", completedLocation);
            }
            //TODO: Re-try logic at the mutation level, with exponential backoff and alerting
            catch (Exception e)
            {
                logger.error("Error sending data at offset {} : {}", e.getMessage());
            }
        }
        mutationFuture = executor.submit(() ->
        {
            int offset = entryLocation;
            for (PartitionUpdate partitionUpdate : mutation.getPartitionUpdates())
            {
                Boolean retry = true;
                try
                {
                    while (retry)
                    {
                        output.emitPartition(partitionUpdate);
                        retry = false;
                    }
                }
                catch (Exception ex)
                {
                    // TODO: bounded number of retries at the partition level.
                }
            }
            logger.debug("Done sending data at offset {}", offset);
            return offset;
        });
    }

    public void stop()
    {
        this.executor.shutdown();
    }
}
