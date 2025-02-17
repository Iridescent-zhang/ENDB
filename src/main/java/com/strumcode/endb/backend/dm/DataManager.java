package com.strumcode.endb.backend.dm;

import com.strumcode.endb.backend.dm.pageCache.PageCache;
import com.strumcode.endb.backend.dm.dataItem.DataItem;
import com.strumcode.endb.backend.dm.logger.Logger;
import com.strumcode.endb.backend.dm.page.PageOne;
import com.strumcode.endb.backend.tm.TransactionManager;

// DM 层提供了三个功能供上层使用，分别是读、插入和修改。修改是通过读出的 DataItem 实现的，于是 DataManager 只需要提供 read() 和 insert() 方法。
public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if(!dm.loadCheckPageOne()) {
            // 通过校验第一页来判断上次数据库是否正常关闭，从而决定是否要执行恢复流程
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        // 重新对第一页生成随机字节
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
