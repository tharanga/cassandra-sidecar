package org.apache.cassandra.sidecar.cdc;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.sidecar.Configuration;



/**
 * Captures changes from from Cassandra commit logs dumps data from SSTables.
 */
public class CDCReaderService implements Host.StateListener
{
    private static final Logger logger = LoggerFactory.getLogger(CDCReaderService.class);
    private Configuration conf;
    private CDCIndexWatcher cdcIndexWatcher;
    private SSTableDumper ssTableDumper;
    private ExecutorService cdcWatcher;

    public CDCReaderService(Configuration conf)
    {
        this.conf = conf;
    }

    public synchronized void start()
    {
        try
        {
            if (!DatabaseDescriptor.isToolInitialized())
            {
                System.setProperty("cassandra.config", conf.getCassandraConfigPath());
                DatabaseDescriptor.toolInitialization();
                Schema.instance.loadFromDisk(false);
            }

            if (!DatabaseDescriptor.isCDCEnabled())
            {
                logger.error("CDC is not enabled");
                return;
            }

            KeyspaceMetadata keyspaceMetadata = Schema.instance.getKeyspaceMetadata(conf.getKeySpace());
            if (keyspaceMetadata == null)
            {
                logger.error("Keyspace {} is not found", conf.getKeySpace());
                return;
            }

            if (keyspaceMetadata.tables.get(conf.getColumnFamily()) == null)
            {
                logger.error("Column family {} is not found under the Keyspace {}", conf.getColumnFamily(),
                        conf.getKeySpace());
                return;
            }

            Schema.instance.load(keyspaceMetadata);
            this.cdcIndexWatcher = new CDCIndexWatcher(this.conf, DatabaseDescriptor.getCDCLogLocation());
            this.ssTableDumper = new SSTableDumper(this.conf);
            cdcWatcher = Executors.newSingleThreadExecutor();
            cdcWatcher.submit(this.cdcIndexWatcher);
            ssTableDumper.dump();
        }
        catch (Exception ex)
        {
            logger.error("Error starting the CDC reader {}", ex.getMessage());
            return;
        }
        logger.info("Started the CDC reader : {} on {}",  DatabaseDescriptor.getCDCSpaceInMB(),
                DatabaseDescriptor.getCDCLogLocation());
    }

    public synchronized void stop()
    {

    }

    @Override
    public void onAdd(Host host)
    {

    }

    @Override
    public void onUp(Host host)
    {

    }

    @Override
    public void onDown(Host host)
    {

    }

    @Override
    public void onRemove(Host host)
    {

    }

    @Override
    public void onRegister(Cluster cluster)
    {

    }

    @Override
    public void onUnregister(Cluster cluster)
    {

    }
}
