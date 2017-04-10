package net.cassandra;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EmbeddedServerExample {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedServerExample.class);

    private static CassandraDaemon cassandraDaemon;

    private static File tmpDir = new File("tmp");
    private static File tmpDataDir = new File("tmp/data/" + UUID.randomUUID().toString());

    public static void main(String args[]) throws IOException {
        // Create temporary data directory.
        if (!tmpDataDir.exists()) {
            LOG.trace("Creating temporary directory for Cassandra server data");
            tmpDataDir.mkdirs();
        } else {
            FileUtils.deleteRecursive(tmpDir);
            tmpDataDir.mkdirs();
        }

        // Validate config file exists
        URL cassandraConfigFileURL = ClassLoader.getSystemResource("cassandra/cassandra.yaml");
        if (cassandraConfigFileURL == null) {
            throw new FileNotFoundException("Cassandra config not available on classpath.");
        }

        // Validate Log4J config exists.
        URL log4JConfigFileURL = ClassLoader.getSystemResource("log4j.xml");
        if (log4JConfigFileURL == null) {
            throw new FileNotFoundException("Log4j config not available on classpath.");
        }

        System.setProperty("cassandra.config", "file:" + cassandraConfigFileURL.getPath());
        System.setProperty("cassandra-foreground","true");
        System.setProperty("log4j.configuration", "file:" + log4JConfigFileURL.getPath());
        System.setProperty("cassandra.storagedir", tmpDataDir.getAbsolutePath());

        // Create required directories for Cassandra and list them
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.createAllDirectories();

        LOG.info("Starting Cassandra server daemon thread");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(new CassandraServerDaemon());

//        executor.shutdown();
//        executor.shutdownNow();
//        FileUtils.deleteRecursive(tmpDir);
        LOG.info("Teardown complete");
    }



    /**
     * Represents running an embedded Cassandra instance via a daemon thread.
     */
    static class CassandraServerDaemon implements Runnable {
        @Override
        public void run() {
            cassandraDaemon = new CassandraDaemon();
            cassandraDaemon.activate();
        }
    }

}
