package com.strumcode.endb.backend.dm;

import com.strumcode.endb.backend.common.AbstractCache;
import com.strumcode.endb.backend.dm.dataItem.DataItem;
import com.strumcode.endb.backend.dm.dataItem.DataItemImpl;
import com.strumcode.endb.backend.dm.logger.Logger;
import com.strumcode.endb.backend.dm.page.Page;
import com.strumcode.endb.backend.dm.page.PageOne;
import com.strumcode.endb.backend.dm.page.PageX;
import com.strumcode.endb.backend.dm.pageCache.PageCache;
import com.strumcode.endb.backend.dm.pageIndex.PageIndex;
import com.strumcode.endb.backend.dm.pageIndex.PageInfo;
import com.strumcode.endb.backend.tm.TransactionManager;
import com.strumcode.endb.backend.utils.Panic;
import com.strumcode.endb.backend.utils.Types;
import com.strumcode.endb.common.Error;

/**
 * DataManager 是 DM 层直接对外提供方法的类，同时，也实现成 DataItem 对象的缓存。
 * 而缓存 DataItem 对象使用的 key，是由页号和页内偏移组成的一个 8 字节无符号整数，页号和偏移各占 4 字节。
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE) {
            // 这里不允许一个数据项超过一个页允许存放的空间
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            // 首先需要写入插入日志，接着才可以通过 pageX 插入数据，并返回插入位置的偏移。
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的 pg 重新插入pIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        // 关闭时设置第一页的字节校验
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 生成 update 日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    // 继承 AbstractCache<DataItem> 要实现的
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    // 继承 AbstractCache<DataItem> 要实现的
    @Override
    protected void releaseForCache(DataItem di) {
        // 需要将 DataItem 写回数据源，由于对文件的读写是以页为单位进行的，只需要将 DataItem 所在的页 release 即可
        di.page().release();
    }

    // 在创建文件时初始化 PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入 PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
    
}
