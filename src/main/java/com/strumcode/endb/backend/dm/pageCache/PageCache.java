package com.strumcode.endb.backend.dm.pageCache;

import com.strumcode.endb.backend.dm.page.Page;
import com.strumcode.endb.backend.utils.Panic;
import com.strumcode.endb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 总的来说就是用来管理页面 page 的 page缓存池
 */
public interface PageCache {
    
    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);                            // 就是 AbstractCache 的 release 【减少缓存的引用计数一次，尝试刷回磁盘】

    void truncateByBgno(int maxPgno);                   // 根据已知的这个文件能产生的最大的页数，截断文件多余的部分
    int getPageNumber();                                // 获得 pageCache 的总页数
    void flushPage(Page pg);                            // 将脏页刷回磁盘

    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path+ PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }
        /**
         * 调用时传来的 memory 是整个 db 允许的最大内存，那除以页的大小 PAGE_SIZE 就能知道这个 pageCache 理论能存放的最大资源数
         */
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path+ PageCacheImpl.DB_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}
