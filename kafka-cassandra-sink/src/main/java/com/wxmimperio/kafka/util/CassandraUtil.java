package com.wxmimperio.kafka.util;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.wxmimperio.kafka.common.CassandraManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wxmimperio on 2017/3/1.
 */
public class CassandraUtil {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraUtil.class);
    private static Session session;

    static {
        session = CassandraManager.getInstance().getSession();
    }

    public static boolean executeSql(String sql) {
        try {
            if (session != null) {
                session.execute(sql);
            }
        } catch (Exception e) {
            LOG.error("Execute CQL error:" + e);
            return false;
        }
        return true;
    }

    public static boolean executeStatement(BatchStatement batchStatement) {
        try {
            if (session != null) {
                session.execute(batchStatement);
            }
        } catch (Exception e) {
            LOG.error("Execute Batch Statement error:" + e);
            return false;
        }
        return true;
    }

    public static PreparedStatement prepareBatch(String sql) {
        PreparedStatement prepareBatch = null;
        try {
            if (session != null) {
                prepareBatch = session.prepare(sql);
            }
        } catch (Exception e) {
            LOG.error("Prepare Statement error:" + e);
        }
        return prepareBatch;
    }

    private void closeSession() {
        if (session != null) {
            session.close();
            session = null;
        }
    }
}
