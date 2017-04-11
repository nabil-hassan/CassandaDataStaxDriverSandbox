package net.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EmbeddedServerExample {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedServerExample.class);

    private static final String CLUSTER_NAME = "EmbeddedCluster";
    private static final String HOST_NAME = "localhost";
    private static File tmpDir = new File("tmp");
    private static File tmpDataDir = new File("tmp/data/" + UUID.randomUUID().toString());
    private static CassandraDaemon cassandraDaemon;
    private static ExecutorService executor;
    private static Cluster cluster;

    public static void main(String args[]) throws IOException {
        // First check Cassandra is not already running
        if (isCassandraRunning()) {
            cluster.close();
            throw new RuntimeException("Cassandra service is already running.");
        }

        // Remove old temporary directories if they exist.
        if (tmpDir.exists()) {
            FileUtils.deleteRecursive(tmpDir);
        }

        // Create temporary data directory.
        if (!tmpDataDir.exists()) {
            LOG.trace("Creating temporary directory for Cassandra server data");
            tmpDataDir.mkdirs();
        } else {
            FileUtils.deleteRecursive(tmpDataDir);
            tmpDataDir.mkdirs();
        }

        // Validate config file exists
        URL cassandraConfigFileURL = ClassLoader.getSystemResource("cassandra/cassandra.yaml");
        if (cassandraConfigFileURL == null) {
            throw new FileNotFoundException("Cassandra config not available on classpath.");
        }

        // Set Cassandra system properties
        System.setProperty("cassandra.config", "file:" + cassandraConfigFileURL.getPath());
        System.setProperty("cassandra-foreground", "true");
        System.setProperty("cassandra.storagedir", tmpDataDir.getAbsolutePath());

        // Create required data directories for Cassandra in the specified storagedir.
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.createAllDirectories();

        // Startup the Cassandra service daemon.
        LOG.info("Starting Cassandra server daemon thread");
        executor = Executors.newSingleThreadExecutor();
        executor.execute(new CassandraServerDaemon());

        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                if (isCassandraRunning()) {
                    LOG.info("Cassandra has started");
                    timer.cancel();
                    cluster = buildCluster();
                    performTest();
                } else {
                    LOG.warn("Cassandra not yet started");
                }
            }
        };
        timer.schedule(task, 5 * 1000, 3 * 1000);
    }

    private static void performTest() {
        LOG.info("Performing test");

        createKeyspaceAndTables();

        populateTables();

        queryTables();

        truncateTables();

        teardownEnvironment();
    }

    /**
     * Builds the cluster that will be used to connect to Cassandra.
     */
    private static Cluster buildCluster() {
        return Cluster.builder()
                .withClusterName(CLUSTER_NAME)
                .addContactPoint(HOST_NAME)
                .build();
    }

    /**
     * Attempts to connect to Cassandra to confirm it has started.
     */
    public static boolean isCassandraRunning() {
        // It is necessary to rebuild a new cluster instance each time,
        // since calling init() more than once on a given instance, results in an IllegalStateException.
        Cluster c = buildCluster();

        try {
            c.init();
            c.close();
        } catch (Exception e) {
            // Another exception type means the cluster is there, but something else is wrong.
            if (e instanceof NoHostAvailableException) {
                return false;
            }
        }

        return true;
    }

    /**
     * Creates the demo keyspace, along with two tables.
     */
    private static void createKeyspaceAndTables() {
        if (cluster == null || cluster.isClosed()) {
            throw new IllegalStateException("Cluster is not initialised.");
        }

        // Obtain a connection
        Session session = cluster.connect();

        // Create the demo keyspace
        String createKeyspaceQuery = "CREATE KEYSPACE demo "
                + "WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};";
        LOG.debug("Creating demo keyspace");
        session.execute(createKeyspaceQuery);

        // These table definitions will be read from config in real code.
        String createOfficesQuery = "CREATE COLUMNFAMILY demo.offices\n" +
                "(\n" +
                "id   uuid PRIMARY KEY,\n" +
                "country text,\n" +
                "city text\n" +
                ")";
        LOG.debug("Creating offices table.");
        session.execute(createOfficesQuery);

        String createEmployeesTableQuery = "CREATE COLUMNFAMILY demo.employees\n" +
                "(\n" +
                "id   uuid,\n" +
                "name text,\n" +
                "role text,\n" +
                "PRIMARY KEY(id, name)\n" +
                ")";
        LOG.debug("Creating employees table.");
        session.execute(createEmployeesTableQuery);

        session.close();
    }

    /**
     * Populates the created tables with some initial data.
     */
    private static void populateTables() {
        LOG.debug("Populating tables");

        Session session = cluster.connect();

        session.execute("INSERT into demo.offices(id, country, city)\n"
                + "VALUES(uuid(), 'US', 'Chicago');");

        session.execute("INSERT into demo.offices(id, country, city)\n"
                + "VALUES(uuid(), 'US', 'Detroit');");

        session.execute("INSERT into demo.offices(id, country, city)\n"
                + "VALUES(uuid(), 'US', 'Washington');");

        session.execute("INSERT into demo.employees(id, name, role)\n"
                + "VALUES(uuid(), 'John Doe', 'Accountant');");

        session.execute("INSERT into demo.employees(id, name, role)\n"
                + "VALUES(uuid(), 'Jane Doe', 'HR');");

        session.execute("INSERT into demo.employees(id, name, role)\n"
                + "VALUES(uuid(), 'James Doe', 'Marketing');");

        session.close();
    }

    /**
     * Query the tables found in the demo keyspace.
     */
    private static void queryTables() {
        LOG.debug("Querying tables");

        Session session = cluster.connect();

        ResultSet employeeRs = session.execute("SELECT * FROM demo.employees");
        for (Row r : employeeRs) {
            LOG.debug("Found employee with id: {}, name: {}, role: {}",
                    r.getUUID(0), r.getString(1), r.getString(2));
        }

        ResultSet officeRs = session.execute("SELECT * FROM demo.offices");
        for (Row r : officeRs) {
            LOG.debug("Found office with id: {}, country: {}, city: {}",
                    r.getUUID(0), r.getString(1), r.getString(2));
        }

        session.close();
    }

    /**
     * Truncates all tables found the demo keyspace.
     */
    private static void truncateTables() {
        Session session = cluster.connect();

        ResultSet tableRs = session.execute("SELECT table_name FROM system_schema.tables\n" +
                "WHERE keyspace_name = 'demo';");

        for (Row r : tableRs) {
            String table = r.getString(0);
            LOG.debug("Truncating table: {}", table);
            session.execute("TRUNCATE demo." + table);
        }

        session.close();
    }

    /**
     * Terminates the executor running the Cassandra Daemon thread, and removes the temporary data directory.
     */
    private static void teardownEnvironment() {
        executor.shutdown();
        executor.shutdownNow();
        cluster.close();
        // this calls System.exit(), completely destroying the JVM process.
        cassandraDaemon.deactivate();
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
