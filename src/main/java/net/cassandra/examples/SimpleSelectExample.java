package net.cassandra.examples;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import net.cassandra.examples.Example;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Objects;

/**
 * Basic example which runs a simple SELECT query against the database, using execute(String).
 */
public class SimpleSelectExample implements Example {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleSelectExample.class);

    private Cluster cluster;

    public SimpleSelectExample(Cluster cluster) {
        this.cluster = Objects.requireNonNull(cluster, "Cluster must be specified");
    }

    public void run() {
        Session session = cluster.connect();
        ResultSet rs = session.execute("SELECT * FROM demo.offices");
        rs.forEach(r -> {
            LOG.info("Found office with uuid: {}, country: {}, city: {}",
                    r.getUUID("id"), r.getString("country"), r.getString("city"));
        });
        session.close();
    }
}
