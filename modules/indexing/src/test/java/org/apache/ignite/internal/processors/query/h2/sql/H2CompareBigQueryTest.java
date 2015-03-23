/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * TODO: Add test class description.
 */
public class H2CompareBigQueryTest extends AbstractH2CompareQueryTest {
    /** {@inheritDoc} */
    @Override protected void setIndexedTypes(CacheConfiguration<?, ?> cc, CacheMode mode) {
        if (mode == CacheMode.PARTITIONED)
            cc.setIndexedTypes(
                Integer.class, OrderT1.class,
                Integer.class, OrderT2.class
            );
        else if (mode == CacheMode.REPLICATED)
            cc.setIndexedTypes(
//                Integer.class, Product.class
            );
        else
            throw new IllegalStateException("mode: " + mode);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void initCacheAndDbData() throws SQLException {
        int idGen = 0;

        final List<OrderT1> ordsT1 = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            int id = idGen++;

            OrderT1 order = new OrderT1();

            ordsT1.add(order);

            pCache.put(order.orderId, order);

            insertInDb(order);
        }

        final List<OrderT2> ordsT2 = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            int id = idGen++;

            OrderT2 order = new OrderT2();

            ordsT2.add(order);

            pCache.put(order.orderId, order);

            insertInDb(order);
        }
    }

    /**
     * @throws Exception If failed.
     */
    protected void checkAllDataEquals() throws Exception {
        compareQueryRes0("select _key, _val, orderId, rootOrderId, date, alias from \"part\".ORDERT1");
        compareQueryRes0("select _key, _val, orderId, rootOrderId, date, alias from \"part\".ORDERT2");
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnionAllOrders() throws Exception {
        compareQueryRes0(
            "   select  date, orderId, rootOrderId " +
                "   from OrderT1 where alias='CUSTOM'" +

                "   union all" +

                "   select  date, orderId, rootOrderId " +
                "   from OrderT2 where alias='CUSTOM'");
    }


    /** {@inheritDoc} */
    @Override protected void initializeH2Schema() throws SQLException {
        Statement st = conn.createStatement();

        st.execute("CREATE SCHEMA \"part\"");
        st.execute("CREATE SCHEMA \"repl\"");

        st.execute("create table \"part\".ORDERT1" +
            "  (" +
            "  _key int not null," +
            "  _val other not null," +
            "  orderId int," +
            "  rootOrderId int," +
            "  date Date, " +
            "  alias varchar(255)" +
            "  )");

        st.execute("create table \"part\".ORDERT2" +
            "  (" +
            "  _key int not null," +
            "  _val other not null," +
            "  orderId int," +
            "  rootOrderId int," +
            "  date Date, " +
            "  alias varchar(255)" +
            "  )");

        conn.commit();
    }

    private void insertInDb(OrderT1 o) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement(
            "insert into \"part\".OrderT1 (_key, _val, orderId, rootOrderId, date, alias) values(?, ?, ?, ?, ?, ?)")) {
            st.setObject(1, o.orderId);
            st.setObject(2, o);
            st.setObject(3, o.orderId);
            st.setObject(4, o.rootOrderId);
            st.setObject(5, o.date);
            st.setObject(6, o.alias);

            st.executeUpdate();
        }
    }

    private void insertInDb(OrderT2 o) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement(
            "insert into \"part\".OrderT1 (_key, _val, orderId, rootOrderId, date, alias) values(?, ?, ?, ?, ?, ?)")) {
            st.setObject(1, o.orderId);
            st.setObject(2, o);
            st.setObject(3, o.orderId);
            st.setObject(4, o.rootOrderId);
            st.setObject(5, o.date);
            st.setObject(6, o.alias);

            st.executeUpdate();
        }
    }

    static class OrderT1 implements Serializable {
        /** Primary key. */
        @QuerySqlField(index = true)
        private int orderId;

        /** Root order ID*/
        @QuerySqlField
        private int rootOrderId;

        /** Date */
        @QuerySqlField
        private Date date = new Date();

        /**  */
        @QuerySqlField
        private String alias = "CUSTOM";

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof OrderT1 && orderId == ((OrderT1)o).orderId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return orderId;
        }
    }

    static class OrderT2 implements Serializable {
        /** Primary key. */
        @QuerySqlField(index = true)
        private int orderId;

        /** Root order ID*/
        @QuerySqlField
        private int rootOrderId;

        /** Date */
        @QuerySqlField
        private Date date = new Date();

        /**  */
        @QuerySqlField
        private String alias = "CUSTOM";

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof OrderT1 && orderId == ((OrderT1)o).orderId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return orderId;
        }
    }
}
