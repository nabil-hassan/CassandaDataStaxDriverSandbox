package net.cassandra.examples;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Simple demonstration of Datastax driver's ORM capabilities.
 */
public class ORMExample {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncSelectExample.class);

    private Cluster cluster;

    public ORMExample(Cluster cluster) {
        this.cluster = Objects.requireNonNull(cluster, "Cluster must be specified");
    }

    public void run() {
        // Create a new entity
        UUID departmentId = UUID.randomUUID();
        Department department = new Department(departmentId, "HR", new Date(), 200,
                Arrays.asList("Salary management", "Hiring", "Complaints"));
        department.setId(departmentId);
        department.setName("HR");
        department.setHeadCount(200);
        department.setCreated(new Date());
        department.setCapabilities(Arrays.asList("Salary management", "Hiring", "Complaints"));

        // Obtain the Mapping manager and save it
        Session session = cluster.newSession();
        MappingManager manager = new MappingManager(session);
        Mapper<Department> mapper = manager.mapper(Department.class);
        LOG.debug("Saving department entity: {}", department);
        mapper.save(department);

        // Start a new session and look up the department.
        Department retrieved = mapper.get(departmentId);
        LOG.info("Retrieved saved department with details: {}", retrieved);

        // Modify and save it
        retrieved.setHeadCount(150);
        mapper.save(retrieved);

        // Look it up to verify the details changed
        Department updated = mapper.get(departmentId);
        LOG.info("Retrieved updated department with details: {}", updated);

        // Now delete it
        mapper.delete(updated);

        // Create several departments, then retrieve only those with a headcount > 1000
        Department department1 = new Department(UUID.randomUUID(), "Accounts", new Date(), 1500, null);
        Department department2 = new Department(UUID.randomUUID(), "Recruitment", new Date(), 200, null);
        Department department3 = new Department(UUID.randomUUID(), "Marketing", new Date(), 2300, null);

        mapper.save(department1);
        mapper.save(department2);
        mapper.save(department3);

        ResultSet results = session.execute("SELECT * FROM demo.departments WHERE head_count > 1000 ALLOW FILTERING");
        Result<Department> departments = mapper.map(results);
        for (Department d : departments) {
            LOG.info("Query found department with headcount > 1000, {}", d);
        }

        mapper.delete(department1);
        mapper.delete(department2);
        mapper.delete(department3);

        session.close();
    }

    /**
     * ORM entity class to represent Department.
     */
    @Table(keyspace = "demo", name = "departments")
    public static class Department {

        @PartitionKey
        @Column(name = "id")
        private UUID id;

        @Column(name = "name")
        private String name;

        @Column(name = "created")
        private Date created;

        @Column(name = "head_count")
        private Integer headCount;

        @Column(name = "capabilities")
        private List<String> capabilities;

        public Department() {
        }

        public Department(UUID id, String name, Date created, Integer headCount, List<String> capabilities) {
            this.id = id;
            this.name = name;
            this.created = created;
            this.headCount = headCount;
            this.capabilities = capabilities;
        }

        public UUID getId() {
            return id;
        }

        public void setId(UUID id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Date getCreated() {
            return created;
        }

        public void setCreated(Date created) {
            this.created = created;
        }

        public Integer getHeadCount() {
            return headCount;
        }

        public void setHeadCount(Integer headCount) {
            this.headCount = headCount;
        }

        public List<String> getCapabilities() {
            return capabilities;
        }

        public void setCapabilities(List<String> capabilities) {
            this.capabilities = capabilities;
        }

        @Override public String toString() {
            return "Department{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", created=" + created +
                    ", headCount=" + headCount +
                    '}';
        }
    }

}
