package com.strumcode.endb.backend.dm.page;

/**
 * 内存中的页
 */
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
