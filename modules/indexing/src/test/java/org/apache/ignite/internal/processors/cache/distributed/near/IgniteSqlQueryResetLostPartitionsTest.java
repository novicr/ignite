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
package org.apache.ignite.internal.processors.cache.distributed.near;


import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import javax.cache.Cache;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class IgniteSqlQueryResetLostPartitionsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                        .setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration()
                                        .setMaxSize(200 * 1024 * 1024)
                                        .setPersistenceEnabled(true)
                        )
        );

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTestsStarted() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /** */
    public static class C1 implements Serializable {
        /** */
        private static final long serialVersionUID = 1L;

        /** */
        public C1(int id) {
            this.id = id;
        }

        /** */
        @QuerySqlField(index = true)
        protected Integer id;
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryWithPartialData() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.cluster().active(true);

        final IgniteCache<Integer, C1> cache = setupCache(ignite);

        SqlQuery<Integer, C1> query = createQuery(cache);

        runQuery(query, cache);

        stopGrid(1);
        stopGrid(2);
        awaitPartitionMapExchange();

        int partialResultCount = runQuery(query, cache);

        assert partialResultCount < 1000;

        startGrid(1);
        startGrid(2);

        // next line never finishes
        // awaitPartitionMapExchange();

        assert cache.lostPartitions().size() > 0;

        try {
            runQuery(query, cache);
            fail("Query is expected to fail even though all partitions are now available.");
        } catch (Exception e) {
            // ignore
        }

        for (int i = 0; i < 1000; i++) {
            cache.get(i); // all 1000 keys are now reachable
        }

        ignite.resetLostPartitions(Collections.singletonList("C1"));
        // after reset query becomes available with full results
        int resultCount = runQuery(query, cache);

        assert resultCount == 1000;


    }

    /**
     *
     */
    private SqlQuery<Integer, C1> createQuery(IgniteCache<Integer, C1> cache) throws InterruptedException {
        String sql = "SELECT C1.* from C1 order by C1.id asc";
        SqlQuery<Integer, C1> qry = new SqlQuery<>(C1.class, sql);
        qry.setDistributedJoins(true);
        return qry;
    }

    private int runQuery(SqlQuery<Integer, C1> qry, IgniteCache<Integer, C1> cache) throws InterruptedException {
        log.info("before query run...");
        Collection<Cache.Entry<Integer, C1>> res = cache.query(qry).getAll();
        log.info("result size: " + res.size());
        return res.size();
    }

    @NotNull
    private IgniteCache<Integer, C1> setupCache(Ignite grid) {
        CacheConfiguration<Integer, C1> c1Conf = new CacheConfiguration<Integer, C1>("C1")
                .setIndexedTypes(Integer.class, C1.class)
                .setBackups(1)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);

        final IgniteCache<Integer, C1> cache = grid.getOrCreateCache(c1Conf);

        for (int i = 0; i < 1000; i++) {
            cache.put(i, new C1(i));
        }
        return cache;
    }
}