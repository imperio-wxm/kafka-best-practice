package com.wxmimperio.kafka.common;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wxmimperio on 2017/7/1.
 */
public class CassandraManager {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraManager.class);
    private static Cluster cluster;
    private static Session session;

    private static class SingletonHolder {
        private static final CassandraManager INSTANCE = new CassandraManager();
    }

    public static CassandraManager getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private CassandraManager() {
        init();
    }

    private void init() {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 4)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 20);
        Cluster.Builder builder = Cluster.builder();
        for (String connect : "".split(",", -1)) {
            builder.addContactPoints(connect);
        }
        cluster = builder.withPoolingOptions(poolingOptions).build();
        LOG.info("Cluster build!");
    }

    public Session getSession() {
        session = cluster.connect();
        return session;
    }

    public void closeSession() {
        if (session != null) {
            session.close();
        }
    }

    public void closeCluster() {
        if (cluster != null) {
            closeSession();
            cluster.close();
        }
    }

    public Cluster getCluster() {
        return cluster;
    }
}
