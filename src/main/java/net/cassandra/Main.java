package net.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import net.cassandra.examples.SimpleSelectExample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Entry point for application.
 *
 * The various examples can be run/not run, by commenting out appropriate lines.
 *
 * Before running this code, you must have installed and configured Cassandra, and created the 'DEMO' keyspace,
 * by running the CQL commands found in file: 'src/main/resources/create-demo-keyspace' against your
 * local Cassandra installation.
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static Cluster cluster;

    public static void main(String[] args) throws Exception {
        // initialise the Cassandra cluster.
        initialiseCluster();

        // run the very basic select example
        SimpleSelectExample sse = new SimpleSelectExample(cluster);
        sse.run();

        // close the cluster now all work complete
        cluster.close();
    }

    public static void initialiseCluster() throws IOException {
        LOG.info("Initialising cluster");

        // Read properties from file.
        Properties properties = new Properties();
        InputStream pStream = ClassLoader.getSystemResourceAsStream("application.properties");
        properties.load(pStream);
        String clusterName = properties.getProperty(PropertyKeys.CLUSTER_NAME);
        String host = properties.getProperty(PropertyKeys.HOST);
        String portStr = properties.getProperty(PropertyKeys.PORT);
        Integer port = portStr == null || portStr.trim().length() == 0
                ? null : Integer.valueOf(properties.getProperty(PropertyKeys.PORT));
        String username = properties.getProperty(PropertyKeys.USERNAME);
        String password = properties.getProperty(PropertyKeys.PASSWORD);

        // Validate mandatory parameters are set.
        if (host == null || host.length() == 0) {
           throw new IllegalArgumentException("The 'host' property must be specified");
        }

        // Build the cluster
        Cluster.Builder builder = Cluster.builder();
        builder.addContactPoint(host);
        if (clusterName != null && clusterName.length() > 0) {
            builder.withClusterName(clusterName);
        }
        if (port != null) {
            builder.withPort(port);
        }
        if (username != null && password != null
                && username.length() > 0 && password.length() > 0) {
            builder.withCredentials(username, password);
        }
        cluster = builder.build();
    }

}
