package org.apache.cassandra.sidecar.cdc;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.sidecar.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public class CommitLogWatcher implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogWatcher.class);
    private WatchService watcher;
    private WatchKey key;
    private Path dir;
    private int prevOffset = 0;
    private CommitLogReader commitLogReader;
    private Path oldPath = null;
    private Path oldIdxPath = null;

    private CDCReader cdcReader;
    private Configuration conf;

    CommitLogWatcher(Configuration conf, String cdcLogPath)
    {
        this.dir = Paths.get(cdcLogPath);
        this.conf = conf;
        this.cdcReader = new CDCReader(conf);
        this.commitLogReader =  new CommitLogReader();
    }

    @Override
    public void run()
    {
        try {
            this.watcher = FileSystems.getDefault().newWatchService();
            this.key = dir.register(watcher, ENTRY_MODIFY);
            while (true) {
                WatchKey aKey = watcher.take();
                if (!key.equals(aKey)) {
                    logger.error("WatchKey not recognized.");
                    continue;
                }
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path relativePath = ev.context();
                    Path absolutePath = dir.resolve(relativePath);
                    processCommitLogSegment(absolutePath);
                    logger.debug("{}: {}", event.kind().name(), absolutePath);
                }
                key.reset();
            }
        } catch (Throwable throwable)
        {
            logger.error("Error when watching the CDC dir : {}", throwable.getMessage());
        }
    }

    private void processCommitLogSegment(Path path)
    {
        logger.debug("Processing a commitlog segment");
        if (!path.toString().endsWith(".idx")) return;
        String spath = path.toString();
        spath = spath.substring(spath.lastIndexOf('/') + 1, spath.lastIndexOf('_')) + ".log";
        Path newPath = Paths.get(path.getParent() + "/" + spath); //"/commitlog/"
        try {
            RandomAccessReader reader = RandomAccessReader.open(path.toFile());
            if (reader == null) return;
            int offset = Integer.parseInt(reader.readLine());
            long segmentId = Long.parseLong(spath.substring(spath.lastIndexOf('-')+1, spath.lastIndexOf('.')));
            reader.close();
            // prevOffset = prevOffset >= offset ? 0 : prevOffset;
            try {
                if (oldPath != null && !oldPath.toString().equals(newPath.toString())) {
                    boolean suc = oldPath.toFile().delete();
                    logger.info("{} the old file {}", suc ? "Deleted" : "Could not delete", oldPath.toString());
                    prevOffset = 0;
                }
                if (oldIdxPath != null && !oldIdxPath.toString().equals(path.toString())) {
                    boolean suc = oldIdxPath.toFile().delete();
                    logger.info("{} the old CDC idx file {}", suc ? "Deleted" : "Could not delete", oldIdxPath);
                }
            } catch (Exception ex)
            {
                logger.error("Error when deleting the old file {} : {}", oldPath.toString(), ex.getMessage());
            }
            logger.info("Reading from the commit log file {} at offset {}", newPath.toString(), prevOffset);
            oldPath = newPath;
            oldIdxPath = path;


                CommitLogPosition clp = new CommitLogPosition(segmentId, prevOffset);
                commitLogReader.readCommitLogSegment(this.cdcReader, newPath.toFile(), clp, -1, false);
            prevOffset = offset;
        } catch (RuntimeException ex) {
            logger.error("Error when processing a commit log segment" + ex.getMessage());
        } catch (Exception ex) {
            logger.error("Error when processing a commit log segment" + ex.getMessage());
        }
        logger.debug("Commitlog segment processed.");
    }

}
