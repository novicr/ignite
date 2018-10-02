package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;


public class IgnitePdsLostPartitionsTest extends GridCommonAbstractTest {

    /**
     * Default cache.
     */
    private static final String CACHE = "cache";

    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

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

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setRebalanceThreadPoolSize(2);

        CacheConfiguration ccfg1 = new CacheConfiguration<>(CACHE)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setIndexedTypes(Integer.class, Integer.class)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setRebalanceBatchesPrefetchCount(2)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("v1", Integer.class.getName());
        fields.put("v2", Integer.class.getName());


        QueryIndex qryIdx = new QueryIndex("v1", true);


        List<CacheConfiguration> cacheCfgs = new ArrayList<>();
        cacheCfgs.add(ccfg1);

        cfg.setCacheConfiguration(cacheCfgs.toArray(new CacheConfiguration[cacheCfgs.size()]));

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4)
                .setCheckpointFrequency(180000)
                .setWalMode(WALMode.LOG_ONLY)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        .setName("dfltDataRegion")
                        .setPersistenceEnabled(true)
                        .setMaxSize(512 * 1024 * 1024)
                );

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setDiscoverySpi(
                new TcpDiscoverySpi()
                        .setIpFinder(IP_FINDER)
        );

        return cfg;
    }


    // novicr

    /**
     * Test that partitions lost is correctly updated.
     *
     * @throws Exception If fails.
     */
    public void testLostPartitionsCountWithTwoNodesRemaining() throws Exception {
        Ignite ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);
        IgniteEx ignite2 = startGrid(2);
        IgniteEx ignite3 = startGrid(3);

        ignite0.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache1 = ignite0.cache(CACHE);

        for (int i = 0; i < 1000; i++)
            cache1.put(i, i);

        ignite1.close();
        ignite2.close();
        //ignite3.close();

        awaitPartitionMapExchange();

        assert !cache1.lostPartitions().isEmpty();
    }

    /**
     * Test that partitions lost is correctly updated.
     *
     * @throws Exception If fails.
     */
    public void testLostPartitionsCountWithOneNodeRemaining() throws Exception {
        Ignite ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);
        IgniteEx ignite2 = startGrid(2);
        IgniteEx ignite3 = startGrid(3);

        ignite0.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache1 = ignite0.cache(CACHE);

        for (int i = 0; i < 1000; i++)
            cache1.put(i, i);

        ignite1.close();
        ignite2.close();
        ignite3.close();

        awaitPartitionMapExchange();

        assert !cache1.lostPartitions().isEmpty();
    }

}
