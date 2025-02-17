package com.strumcode.endb.backend.dm.page;

import com.strumcode.endb.backend.dm.pageCache.PageCache;
import com.strumcode.endb.backend.utils.Parser;

import java.util.Arrays;

/**
 * PageX 管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节，表示这一页的空闲位置  FSO（Free Space Offset） 的偏移
 */
public class PageX {
    
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    // 设置 FSO 偏移到 raw 里
    private static void setFSO(byte[] raw, short fso) {
        System.arraycopy(Parser.short2Byte(fso), 0, raw, OF_FREE, OF_DATA);
    }

    // 获取此页的 FSO 偏移
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }


    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, OF_DATA));
    }

    // 将 raw 接着写在 pg 中，返回 raw 插入的位置
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(), (short)(offset + raw.length));
        return offset;
    }

    // 获取页面的空闲空间大小，注意不是空闲空间偏移
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    // Keypoint 两个函数 recoverInsert() 和 recoverUpdate() 用于在数据库崩溃后重新打开时，恢复例程直接插入数据以及修改数据使用。

    // 将 raw 插入 pg 中的 offset 位置，并将 pg 的 FSO 设置为较大值
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset+raw.length));
        }
    }

    // 将 raw 插入 pg 中的 offset 位置，不更新 FSO
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
