package net.cassandra.examples;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Basic example which runs a SELECT query against the database, using the asynchronous form of execute(String).
 */
public class AsyncSelectExample {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncSelectExample.class);

    private Cluster cluster;

    public AsyncSelectExample(Cluster cluster) {
        this.cluster = Objects.requireNonNull(cluster, "Cluster must be specified");
    }

    public void run() {
        Session session = cluster.connect();
        ResultSetFuture rsf = session.executeAsync("SELECT * FROM demo.offices");
        Futures.addCallback(rsf, new FutureCallback<ResultSet>() {
                    @Override
                    public void onSuccess(ResultSet rs) {
                        rs.forEach(r -> {
                            LOG.info("Asynchronously found office with uuid: {}, country: {}, city: {}",
                                    r.getUUID("id"), r.getString("country"), r.getString("city"));
                        });
                        session.close();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        LOG.error("Error attempting select offices async query", t);
                    }
                }
        );
    }
}
