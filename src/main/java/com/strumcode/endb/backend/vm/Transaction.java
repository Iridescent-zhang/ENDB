package com.strumcode.endb.backend.vm;

import com.strumcode.endb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

// vm 对一个事务的抽象
public class Transaction {
    public long xid;
    public int level;                                      // 用来判断是否可以版本跳跃（读提交可以版本跳跃，可重复读不行） int level = begin.isRepeatableRead?1:0;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;                            // 自动中断

    /**
     * @param active    保存着当前所有 active 的事务
     */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        // level != 0 即为可重复读隔离级别，所以保存快照
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
