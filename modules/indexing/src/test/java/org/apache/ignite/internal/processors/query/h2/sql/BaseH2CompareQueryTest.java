/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Base set of queries to compare query results from h2 database instance and mixed ignite caches (replicated and partitioned)
 * which have the same data models and data content.
 */
public class BaseH2CompareQueryTest extends AbstractH2CompareQueryTest {
    /** {@inheritDoc} */
    @Override protected void setIndexedTypes(CacheConfiguration<?, ?> cc, CacheMode mode) {
        if (mode == CacheMode.PARTITIONED)
            cc.setIndexedTypes(
                Integer.class, Organization.class,
                CacheAffinityKey.class, Person.class,
                CacheAffinityKey.class, Purchase.class
            );
        else if (mode == CacheMode.REPLICATED)
            cc.setIndexedTypes(
                Integer.class, Product.class
            );
        else
            throw new IllegalStateException("mode: " + mode);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void initCacheAndDbData() throws SQLException {
        int idGen = 0;

        // Organizations.
        List<Organization> organizations = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            int id = idGen++;

            Organization org = new Organization(id, "Org" + id);

            organizations.add(org);

            pCache.put(org.id, org);

            insertInDb(org);
        }

        // Persons.
        List<Person> persons = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            int id = idGen++;

            Person person = new Person(id, organizations.get(i % organizations.size()),
                "name" + id, "lastName" + id, id * 100.0);

            // Add a Person without lastname.
            if (id == organizations.size() + 1)
                person.lastName = null;

            persons.add(person);

            pCache.put(person.key(), person);

            insertInDb(person);
        }

        // Products.
        List<Product> products = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            int id = idGen++;

            Product product = new Product(id, "Product" + id, id*1000);

            products.add(product);

            rCache.put(product.id, product);

            insertInDb(product);
        }

        // Purchases.
        for (int i = 0; i < products.size() * 2; i++) {
            int id = idGen++;

            Purchase purchase = new Purchase(id, products.get(i % products.size()), persons.get(i % persons.size()));

            pCache.put(purchase.key(), purchase);

            insertInDb(purchase);
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkAllDataEquals() throws Exception {
        compareQueryRes0("select _key, _val, id, name from \"part\".Organization");

        compareQueryRes0("select _key, _val, id, firstName, lastName, orgId, salary from \"part\".Person");

        compareQueryRes0("select _key, _val, id, personId, productId from \"part\".Purchase");

        compareQueryRes0(rCache, "select _key, _val, id, name, price from \"repl\".Product");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleReplSelect() throws Exception {
        compareQueryRes0("select id, name, price from \"repl\".Product");
    }

    /**
     * @throws Exception If failed.
     */
    public void testParamSubstitution() throws Exception {
        compareQueryRes0("select ? from \"part\".Person", "Some arg");
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullParamSubstitution() throws Exception {
        List<List<?>> rs1 = compareQueryRes0("select id from \"part\".Person where lastname is ?", null);

        // Ensure we find something.
        assertNotSame(0, rs1.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testEmptyResult() throws Exception {
        compareQueryRes0("select id from \"part\".Person where 0 = 1");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlQueryWithAggregation() throws Exception {
        compareQueryRes0("select avg(salary) from \"part\".Person, \"part\".Organization where Person.orgId = Organization.id and "
            + "lower(Organization.name) = lower(?)", "Org1");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlFieldsQuery() throws Exception {
        compareQueryRes0("select concat(firstName, ' ', lastName) from \"part\".Person");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlFieldsQueryWithJoin() throws Exception {
        compareQueryRes0("select concat(firstName, ' ', lastName), "
            + "Organization.name from \"part\".Person, \"part\".Organization where "
            + "Person.orgId = Organization.id");
    }

    /**
     * @throws Exception If failed.
     */
    public void testOrdered() throws Exception {
        compareOrderedQueryRes0("select firstName, lastName" +
                " from \"part\".Person" +
                " order by lastName, firstName"
        );
    }

    /**
     * //TODO Investigate.
     *
     * @throws Exception If failed.
     */
    public void testSimpleJoin() throws Exception {
        // Have expected results.
        compareQueryRes0("select id, firstName, lastName" +
            "  from \"part\".Person" +
            "  where Person.id = ?", 3);

        // Ignite cache return 0 results...
        compareQueryRes0("select Person.firstName" +
            "  from \"part\".Person, \"part\".Purchase" +
            "  where Person.id = ?", 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleReplicatedSelect() throws Exception {
        compareQueryRes0(rCache, "select id, name from \"repl\".Product");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCache() throws Exception {
        //TODO Investigate (should be 20 results instead of 0).
        compareQueryRes0("select firstName, lastName" +
            "  from \"part\".Person, \"part\".Purchase" +
            "  where Person.id = Purchase.personId");

        //TODO Investigate.
        compareQueryRes0("select concat(firstName, ' ', lastName), Product.name " +
            "  from \"part\".Person, \"part\".Purchase, \"repl\".Product " +
            "  where Person.id = Purchase.personId and Purchase.productId = Product.id" +
            "  group by Product.id");

        //TODO Investigate.
        compareQueryRes0("select concat(firstName, ' ', lastName), count (Product.id) " +
            "  from \"part\".Person, \"part\".Purchase, \"repl\".Product " +
            "  where Person.id = Purchase.personId and Purchase.productId = Product.id" +
            "  group by Product.id");
    }

    /** {@inheritDoc} */
    @Override protected void initializeH2Schema() throws SQLException {
        Statement st = conn.createStatement();

        st.execute("CREATE SCHEMA \"part\"");
        st.execute("CREATE SCHEMA \"repl\"");

        st.execute("create table \"part\".ORGANIZATION" +
            "  (_key int not null," +
            "  _val other not null," +
            "  id int unique," +
            "  name varchar(255))");

        st.execute("create table \"part\".PERSON" +
            "  (_key other not null ," +
            "   _val other not null ," +
            "  id int unique, " +
            "  firstName varchar(255), " +
            "  lastName varchar(255)," +
            "  orgId int not null," +
            "  salary double )");

        st.execute("create table \"repl\".PRODUCT" +
            "  (_key int not null ," +
            "   _val other not null ," +
            "  id int unique, " +
            "  name varchar(255), " +
            "  price int)");

        st.execute("create table \"part\".PURCHASE" +
            "  (_key other not null ," +
            "   _val other not null ," +
            "  id int unique, " +
            "  personId int, " +
            "  productId int)");

        conn.commit();
    }
    
    /**
     * Insert {@link Organization} at h2 database.
     *
     * @param org Organization.
     * @throws SQLException If exception.
     */
    private void insertInDb(Organization org) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement(
            "insert into \"part\".ORGANIZATION (_key, _val, id, name) values(?, ?, ?, ?)")) {
            st.setObject(1, org.id);
            st.setObject(2, org);
            st.setObject(3, org.id);
            st.setObject(4, org.name);

            st.executeUpdate();
        }
    }

    /**
     * Insert {@link Person} at h2 database.
     *
     * @param p Person.
     * @throws SQLException If exception.
     */
    private void insertInDb(Person p) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement("insert into \"part\".PERSON " +
            "(_key, _val, id, firstName, lastName, orgId, salary) values(?, ?, ?, ?, ?, ?, ?)")) {
            st.setObject(1, p.key());
            st.setObject(2, p);
            st.setObject(3, p.id);
            st.setObject(4, p.firstName);
            st.setObject(5, p.lastName);
            st.setObject(6, p.orgId);
            st.setObject(7, p.salary);

            st.executeUpdate();
        }
    }

    /**
     * Insert {@link Product} at h2 database.
     *
     * @param p Product.
     * @throws SQLException If exception.
     */
    private void insertInDb(Product p) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement(
            "insert into \"repl\".PRODUCT (_key, _val, id, name, price) values(?, ?, ?, ?, ?)")) {
            st.setObject(1, p.id);
            st.setObject(2, p);
            st.setObject(3, p.id);
            st.setObject(4, p.name);
            st.setObject(5, p.price);

            st.executeUpdate();
        }
    }

    /**
     * Insert {@link Purchase} at h2 database.
     *
     * @param p Purchase.
     * @throws SQLException If exception.
     */
    private void insertInDb(Purchase p) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement(
            "insert into \"part\".PURCHASE (_key, _val, id, personId, productId) values(?, ?, ?, ?, ?)")) {
            st.setObject(1, p.key());
            st.setObject(2, p);
            st.setObject(3, p.id);
            st.setObject(4, p.personId);
            st.setObject(5, p.productId);

            st.executeUpdate();
        }
    }

    /**
     * Person class. Stored at partitioned cache.
     */
    private static class Person implements Serializable {
        /** Person ID (indexed). */
        @QuerySqlField(index = true)
        private int id;

        /** Organization ID (indexed). */
        @QuerySqlField(index = true)
        private int orgId;

        /** First name (not-indexed). */
        @QuerySqlField
        private String firstName;

        /** Last name (not indexed). */
        @QuerySqlField
        private String lastName;

        /** Salary (indexed). */
        @QuerySqlField(index = true)
        private double salary;

        /**
         * Constructs person record.
         *
         * @param org Organization.
         * @param firstName First name.
         * @param lastName Last name.
         * @param salary Salary.
         */
        Person(int id, Organization org, String firstName, String lastName, double salary) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.salary = salary;
            orgId = org.id;
        }

        /**
         * @return Custom affinity key to guarantee that person is always collocated with organization.
         */
        public CacheAffinityKey<Integer> key() {
            return new CacheAffinityKey<>(id, orgId);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof Person && id == ((Person)o).id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person [firstName=" + firstName +
                ", lastName=" + lastName +
                ", id=" + id +
                ", orgId=" + orgId +
                ", salary=" + salary + ']';
        }
    }

    /**
     * Organization class. Stored at partitioned cache.
     */
    private static class Organization implements Serializable {
        /** Organization ID (indexed). */
        @QuerySqlField(index = true)
        private int id;

        /** Organization name (indexed). */
        @QuerySqlField(index = true)
        private String name;

        /**
         * Create Organization.
         *
         * @param id Organization ID.
         * @param name Organization name.
         */
        Organization(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof Organization && id == ((Organization)o).id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization [id=" + id + ", name=" + name + ']';
        }
    }

    /**
     * Product class. Stored at replicated cache.
     */
    private static class Product implements Serializable {
        /** Primary key. */
        @QuerySqlField(index = true)
        private int id;

        /** Product name. */
        @QuerySqlField
        private String name;

        /** Product price */
        @QuerySqlField
        private int price;

        /**
         * Create Product.
         *
         * @param id Product ID.
         * @param name Product name.
         * @param price Product price.
         */
        Product(int id, String name, int price) {
            this.id = id;
            this.name = name;
            this.price = price;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof Product && id == ((Product)o).id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Product [id=" + id + ", name=" + name + ", price=" + price + ']';
        }
    }

    /**
     * Purchase class. Stored at partitioned cache.
     */
    private static class Purchase implements Serializable {
        /** Primary key. */
        @QuerySqlField(index = true)
        private int id;

        /** Product ID. */
        @QuerySqlField
        private int productId;

        /** Person ID. */
        @QuerySqlField
        private int personId;

        /**
         * Create Purchase.
         *
         * @param id Purchase ID.
         * @param product Purchase product.
         * @param person Purchase person.
         */
        Purchase(int id, Product product, Person person) {
            this.id = id;
            productId = product.id;
            personId = person.id;
        }

        /**
         * @return Custom affinity key to guarantee that purchase is always collocated with person.
         */
        public CacheAffinityKey<Integer> key() {
            return new CacheAffinityKey<>(id, personId);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof Purchase && id == ((Purchase)o).id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Purchase [id=" + id + ", productId=" + productId + ", personId=" + personId + ']';
        }
    }
}
