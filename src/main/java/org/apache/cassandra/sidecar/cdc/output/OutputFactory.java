package org.apache.cassandra.sidecar.cdc.output;

import org.apache.cassandra.sidecar.Configuration;

/**
 * Factory for creating concrete PartitionUpdate producers
 */
public class OutputFactory
{
    public static Output getOutput(Configuration conf)
    {
        switch (conf.getOutputType())
        {
            case "kafka": return new KafkaOutput(conf);
            case "console": return new ConsoleOutput();
            default: return null;
        }
    }
}
