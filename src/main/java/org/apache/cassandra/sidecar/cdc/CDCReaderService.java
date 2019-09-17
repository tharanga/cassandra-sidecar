package org.apache.cassandra.sidecar.cdc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Singleton;
import javax.naming.ConfigurationException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.google.inject.Inject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.sidecar.CQLSession;
import org.apache.cassandra.sidecar.Configuration;


/**
 * Cassandra's real-time change data capture service.
 */
@Singleton
public class CDCReaderService implements Host.StateListener
{
    private static final Logger logger = LoggerFactory.getLogger(CDCReaderService.class);
    private Configuration conf;
    private CDCIndexWatcher cdcIndexWatcher;
    private SSTableDumper ssTableDumper;
    private ExecutorService cdcWatcher;
    private CDCSchemaChangeListener cdcSchemaChangeListener;

    private final CQLSession session;

    @Inject
    public CDCReaderService(Configuration config, CQLSession session,
                            CDCSchemaChangeListener schemaChangeListener)
    {
        this.conf = config;
        this.session = session;
        this.cdcSchemaChangeListener = schemaChangeListener;
        this.session.getLocalCql().getCluster().register(this.cdcSchemaChangeListener);
    }

    public synchronized void start()
    {
        try
        {
            if (conf == null)
            {
                throw new ConfigurationException("Configuration is not set for the CDC reader");
            }

            System.setProperty("cassandra.config", conf.getCassandraConfigPath());

            if (!DatabaseDescriptor.isDaemonInitialized())
            {
                DatabaseDescriptor.forceStaticInitialization();
                Schema.instance.loadFromDisk(false);
            }

            if (!DatabaseDescriptor.isCDCEnabled())
            {
                logger.error("CDC is not enabled");
                return;
            }

            // Start reading the current commit log.
            this.cdcIndexWatcher = new CDCIndexWatcher(this.conf, DatabaseDescriptor.getCDCLogLocation());
            cdcWatcher = Executors.newSingleThreadExecutor();
            cdcWatcher.submit(this.cdcIndexWatcher);

            // Take a snapshot from existing CDC enabled tables.
            for (String keySpace : Schema.instance.getKeyspaces())
            {
                KeyspaceMetadata keyspaceMetadata = Schema.instance.getKSMetaData(keySpace);
                if (keyspaceMetadata == null)
                {
                    return;
                }
                for (CFMetaData tableMetadata : keyspaceMetadata.tablesAndViews())
                {
                    if (!tableMetadata.params.cdc)
                    {
                        continue;
                    }
                    this.ssTableDumper = new SSTableDumper(this.conf, tableMetadata.ksName, tableMetadata.cfName);
                    ssTableDumper.dump();
                }
            }
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
       logger.info("Stopping CDC reader");
       this.cdcIndexWatcher.stop();
       this.cdcWatcher.shutdown();
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
