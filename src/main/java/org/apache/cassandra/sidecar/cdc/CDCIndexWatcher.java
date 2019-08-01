package org.apache.cassandra.sidecar.cdc;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.Configuration;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

/**
 * Watches CDC index and produce commit log offsets to be read and processed.
 */
public class CDCIndexWatcher implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(CDCIndexWatcher.class);
    private WatchService watcher;
    private WatchKey key;
    private Path dir;
    private Configuration conf;
    private org.apache.cassandra.sidecar.cdc.CommitLogReader commitLogReader;
    private BlockingQueue<Path> blockingQueue;

    CDCIndexWatcher(Configuration conf, String cdcLogPath)
    {
        this.dir = Paths.get(cdcLogPath);
        this.conf = conf;
        this.blockingQueue = new LinkedBlockingDeque<>(); //TODO : currently this is unbounded, bound it and add alerts.
        this.commitLogReader = new CommitLogReader(conf, this.blockingQueue);
    }

    @Override
    public void run()
    {
        try
        {
            this.commitLogReader.start();
            this.watcher = FileSystems.getDefault().newWatchService();
            this.key = dir.register(watcher, ENTRY_MODIFY);
            while (true)
            {
                WatchKey aKey = watcher.take();
                if (!key.equals(aKey))
                {
                    logger.error("WatchKey not recognized.");
                    continue;
                }
                for (WatchEvent<?> event : key.pollEvents())
                {
                    WatchEvent.Kind<?> kind = event.kind();
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path relativePath = ev.context();
                    Path absolutePath = dir.resolve(relativePath);
                    logger.debug("Event type : {}, Path : {}", event.kind().name(), absolutePath);
                    this.blockingQueue.add(absolutePath);
                }
                key.reset();
            }
        }
        catch (Throwable throwable)
        {
            logger.error("Error when watching the CDC dir : {}", throwable.getMessage());
        }
    }
}
