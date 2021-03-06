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
import java.util.Collection;

/**
 *
 */
public class IgniteSqlQueryWithLostPartitionsTest extends GridCommonAbstractTest {
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
        startGrids(4);

        grid(0).cluster().active(true);

        doQueryWithPartialData();
    }

    /**
     *
     */
    private void doQueryWithPartialData() throws InterruptedException {
        final IgniteCache<Integer, C1> cache = setupCaches();

        String sql = "SELECT C1.* from C1 order by C1.id asc";

        SqlQuery<Integer, C1> qry = new SqlQuery<>(C1.class, sql);

        qry.setDistributedJoins(true);

        log.info("before query run...");

        Collection<Cache.Entry<Integer, C1>> res = cache.query(qry).getAll();

        log.info("result size: " + res.size());

        stopGrid(1);
        stopGrid(2);
        awaitPartitionMapExchange();

        Collection<Cache.Entry<Integer, C1>> resPartial = cache.query(qry).getAll();

        log.info("result size: " + resPartial.size());


    }

    @NotNull
    private IgniteCache<Integer, C1> setupCaches() {
        CacheConfiguration<Integer, C1> c1Conf = new CacheConfiguration<Integer, C1>("C1")
                .setIndexedTypes(Integer.class, C1.class)
                .setBackups(1)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);

        final IgniteCache<Integer, C1> cache = grid(0).getOrCreateCache(c1Conf);

        for (int i = 0; i < 1000; i++) {
            cache.put(i, new C1(i));
        }
        return cache;
    }
}