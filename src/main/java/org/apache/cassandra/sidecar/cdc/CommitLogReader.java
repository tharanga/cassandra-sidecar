package org.apache.cassandra.sidecar.cdc;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.io.util.RandomAccessReader;

import org.apache.cassandra.sidecar.Configuration;

/**
 * Reads a Cassandra commit log. Read offsets are provided by the CdcIndexWatcher
 */
public class CommitLogReader
{

    private static final Logger logger = LoggerFactory.getLogger(CommitLogReader.class);
    private ExecutorService executor;
    private BlockingQueue<Path> blockingQueue;
    private int prevOffset = 0;
    private org.apache.cassandra.db.commitlog.CommitLogReader commitLogReader;
    private Path oldPath = null;
    private Path oldIdxPath = null;
    private MutationHandler mutationHandler;
    private boolean running;


    CommitLogReader(Configuration conf, BlockingQueue blockingQueue)
    {
        this.executor = Executors.newSingleThreadExecutor(); //TODO: Single reader, can be multiple readers, but watch
                                                        // for the commit log deletion process, it has to be changed.
        this.blockingQueue = blockingQueue;
        this.commitLogReader = new org.apache.cassandra.db.commitlog.CommitLogReader();
        this.mutationHandler = new MutationHandler(conf);
        this.running = true;
    }

    public void start()
    {
        executor.submit(() ->
        {
            try
            {
                while (this.running)
                {
                    Path path = this.blockingQueue.take();
                    processCDCWatchEvent(path);
                }
            }
            catch (InterruptedException e)
            {
                logger.error("Error handling the CDC watch event : {} ", e.getMessage());
            }
            return;
        });
    }

    private void processCDCWatchEvent(Path path)
    {
        logger.debug("Processing a commitlog segment");
        if (!path.toString().endsWith(".idx")) return;
        String spath = path.toString();
        spath = spath.substring(spath.lastIndexOf('/') + 1, spath.lastIndexOf('_')) + ".log";
        Path newPath = Paths.get(path.getParent() + "/" + spath); //"/commitlog/"
        try
        {
            RandomAccessReader reader = RandomAccessReader.open(path.toFile());
            if (reader == null) return;
            int offset = Integer.parseInt(reader.readLine());
            long segmentId = Long.parseLong(spath.substring(spath.lastIndexOf('-') + 1, spath.lastIndexOf('.')));
            reader.close();
            // prevOffset = prevOffset >= offset ? 0 : prevOffset;
            try
            {
                if (oldPath != null && !oldPath.toString().equals(newPath.toString()))
                {
                    boolean suc = oldPath.toFile().delete();
                    logger.info("{} the old file {}", suc ? "Deleted" : "Could not delete", oldPath.toString());
                    prevOffset = 0;
                }
                if (oldIdxPath != null && !oldIdxPath.toString().equals(path.toString()))
                {
                    boolean suc = oldIdxPath.toFile().delete();
                    logger.info("{} the old CDC idx file {}", suc ? "Deleted" : "Could not delete", oldIdxPath);
                }
            }
            catch (Exception ex)
            {
                logger.error("Error when deleting the old file {} : {}", oldPath.toString(), ex.getMessage());
            }
            logger.info("Reading from the commit log file {} at offset {}", newPath.toString(), prevOffset);
            oldPath = newPath;
            oldIdxPath = path;


            CommitLogPosition clp = new CommitLogPosition(segmentId, prevOffset);
            commitLogReader.readCommitLogSegment(this.mutationHandler, newPath.toFile(), clp, -1,
                    false);
            prevOffset = offset;
        }
        catch (Exception ex)
        {
            logger.error("Error when processing a commit log segment : {}", ex.getMessage());
        }
        logger.debug("Commit log segment processed.");

    }

    public void stop()
    {
        this.running = false;
        this.executor.shutdown();
        this.mutationHandler.stop();
    }
}
