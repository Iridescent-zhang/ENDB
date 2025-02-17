package com.strumcode.endb.backend.vm;

import com.strumcode.endb.backend.dm.DataManager;
import com.strumcode.endb.backend.tm.TransactionManager;

/**
 * VM 基于两段锁协议实现了调度序列的可串行化（多事务时事务间通过 2PL 实现串行），并实现了 MVCC 以消除读写阻塞。同时实现了两种隔离级别。
 * Version Manager 是事务和数据版本的管理核心。
 */
public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }

}
