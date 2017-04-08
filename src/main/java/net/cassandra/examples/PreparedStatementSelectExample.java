package net.cassandra.examples;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Basic example which runs a simple SELECT query against the database, using a PreparedStatement.
 */
public class PreparedStatementSelectExample {

    private static final Logger LOG = LoggerFactory.getLogger(PreparedStatementSelectExample.class);

    private Cluster cluster;

    public PreparedStatementSelectExample(Cluster cluster) {
        this.cluster = Objects.requireNonNull(cluster, "Cluster must be specified");
    }

    public void run() {
        Session session = cluster.connect();

        PreparedStatement ps = session.prepare("SELECT * FROM demo.offices WHERE country = :country ALLOW FILTERING");

        BoundStatement bs = ps.bind().setString("country", "US");


        ResultSet rs = session.execute(bs);
        rs.forEach(r -> {
            LOG.info("Used prepared statement to find US offices in city: {}, with uuid: {}",
                    r.getString("city"), r.getUUID("id"));
        });

        session.close();
    }


}
