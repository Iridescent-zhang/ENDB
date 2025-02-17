package com.strumcode.endb.backend.dm.dataItem;

import com.strumcode.endb.backend.dm.DataManagerImpl;
import com.strumcode.endb.backend.dm.page.Page;
import com.strumcode.endb.backend.common.SubArray;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法     // 删除一个 DataItem，只需要简单地将其有效位设置为 0。
 * DataSize  2字节，标识 Data 的长度
 */
public class DataItemImpl implements DataItem {

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    /**
     * raw 是描述这个 dataitem 在页中的位置的
     * raw.raw 是 page 的 data
     * raw.start 是这个 dataitem 的数据在页中的起始位置
     * raw.end 是这个 dataitem 的数据在页中的结尾位置
     */
    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dm;             // 保存一个 dm 的引用是因为其释放依赖 dm 的释放（dm 同时实现了缓存接口，用于缓存 DataItem），以及修改数据时落日志。
    private long uid;                       // Types.addressToUid(pg.getPageNumber(), offset);
    private Page pg;

    // uid 是通过 Types.addressToUid(pg.getPageNumber(), offset) 得来的，在 updatelog 中有用
    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    public boolean isValid() {
        return raw.raw[raw.start+OF_VALID] == (byte)0;
    }

    // Keypoint 该方法返回的形式是 SubArray，这个在页层面是数据共享的，并且返回的是这个 dataItem 的纯数据，不包含其他 dataItem 的格式，这个纯数据的格式就是 Entry
    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start+OF_DATA, raw.end);
    }

    /**
     * 在上层模块试图对 DataItem 进行修改时，需要遵循一定的流程：
     * 在修改之前需要调用 before() 方法，想要撤销修改时，调用 unBefore() 方法，在修改完成后，调用 after() 方法。
     * 整个流程，主要是为了保存前相数据，并及时落日志。DM 会保证对 DataItem 的修改是原子性的。
     */
    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        // 从 oldRaw 恢复数据到 raw.raw，这是在修改后如果发生错误或需要回滚时将数据恢复到它的初始状态。
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    @Override
    public void after(long xid) {
        // 记录当前数据项的日志信息，通过日志记录可以实现事务的持久化和恢复机制。
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
    
}
