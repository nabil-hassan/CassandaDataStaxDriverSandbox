package net.cassandra.examples;

import com.datastax.driver.core.*;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * This example demonstrates how paging state information can be saved and reused, allowing for a query to retrieve
 * the next batch(es) of rows, that were not retrieved by the previous run of the query.
 */
public class PagingSelectExample {

    private static final Logger LOG = LoggerFactory.getLogger(PagingSelectExample.class);

    private Cluster cluster;
    private PreparedStatement selectEmployeePS;

    public PagingSelectExample(Cluster cluster) {
        this.cluster = Objects.requireNonNull(cluster, "Cluster must be specified");
    }

    public PagingState run() {
        insertEmployeeData();

        // Get the first 10 results.
        return queryEmployeesPaged(null);
    }

    /**
     * Creates 100 records in the employees table.
     */
    private void insertEmployeeData() {
        Session session = cluster.connect();

        PreparedStatement ps = session.prepare(
                "INSERT INTO demo.employees(id, name, role) VALUES(:id, :name, :role)");

        for (int i = 0; i < 100; i++) {
            BoundStatement bs = ps.bind().setInt("id", i).setString("name", "employee " + i)
                    .setString("role", "role " + i);
            session.execute(bs);
            LOG.debug("Inserted employee {}", i);
        }
        session.close();
    }

    /**
     * Performs a paged query to retrieve employee details.
     *
     * @param state the paging state - if null, the first page of the query is read, otherwise the next is read.
     * @return the paging state, after the current page has been read.
     */
    public PagingState queryEmployeesPaged(PagingState state) {
        Session session = cluster.connect();
        if (selectEmployeePS == null) {
            selectEmployeePS = session.prepare("SELECT id, name, role FROM demo.employees");
        }

        BoundStatement bs = selectEmployeePS.bind();
        if (state != null) {
            bs.setPagingState(state);
        } else {
            // No pages remain, or this is the first time the query has been performed, so perform the query afresh.
        }
        bs.setFetchSize(10);

        ResultSet rs = session.execute(bs);
        int remaining = rs.getAvailableWithoutFetching();
        for (Row r : rs) {
            remaining--;
            LOG.debug("Paged query returned employee with id: {}, name: {}, role: {}",
                    r.getInt(0), r.getString(1), r.getString(2));
            if (remaining == 0) {
                break;
            }
        }

        session.close();

        return rs.getExecutionInfo().getPagingState();
    }

}
