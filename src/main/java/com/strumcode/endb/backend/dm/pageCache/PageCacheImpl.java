package com.strumcode.endb.backend.dm.pageCache;

import com.strumcode.endb.backend.common.AbstractCache;
import com.strumcode.endb.backend.dm.page.Page;
import com.strumcode.endb.backend.dm.page.PageImpl;
import com.strumcode.endb.backend.utils.Panic;
import com.strumcode.endb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;                       // 当前整个 pageCache 的总共的页的数目

    /**
     * @param file  ".db"文件
     * @param fileChannel  从 file.getChannel() 得到的
     * @param maxResource  (int)memory/PAGE_SIZE（memory 是 DB 设置的内存）
     */
    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        /**
         * Keypoint 为什么对最小资源数目有要求？
         * 这里指的是 PageCache 里面能存放页面的最大数目，如果整个 DB 设置的内存很小，甚至连 10 个 page 都放不下，那直接报 内存太小异常 好了
         */
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            // the length of this file, measured in bytes.
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        /**
         * 用当前 ".db" 文件大小除以 page 页大小得到现在的 pageCache 总共有几页
         * Keypoint 这里也能知道，pageCache 中的各个 page 在 ".db" 文件中的位置是按顺序排列的
         */
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);
        return pgno;
    }

    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);
    }

    /**
     * 来自抽象缓存框架 AbstractCache<Page>
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buf.array(), this);
    }

    /**
     * 来自抽象缓存框架 AbstractCache<Page>
     */
    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }

    public void release(Page page) {
        release((long)page.getPageNumber());
    }

    public void flushPage(Page pg) {
        flush(pg);
    }

    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            /**
             * Page 对象中包含的数据，例如 boolean dirty，通常不会直接存储在与 Page 数据（即字节数组）关联的文件中。在实际实现过程中，Page 对象可能包含元数据（如 dirty 标志），但是这些数据通常只是在内存中存在，用于管理和追踪 Page 的状态。
             * 而当我们要把 Page 的数据写入文件时，真正写入的是 pg.getData() 返回的字节数组，这样可以确保我们写入的数据就是我们期望的 Page 内容。
             * fc.position(offset); 设置的是 Page 的字节数据在文件中的写入位置，而这个位置的计算不需要考虑 Page 对象中的元数据，因为这些元数据通常不包含在写入文件的数据中。
             */
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    // 根据已知的这个文件能产生的最大的页数，截断多余的部分
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 获得 pageCache 的总页数
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    private static long pageOffset(int pgno) {
        // 页号从 1 开始
        return (pgno-1) * PAGE_SIZE;
    }
    
}
