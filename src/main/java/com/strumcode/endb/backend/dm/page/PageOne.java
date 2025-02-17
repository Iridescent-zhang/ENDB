package com.strumcode.endb.backend.dm.page;

import com.strumcode.endb.backend.dm.pageCache.PageCache;
import com.strumcode.endb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 数据库 ".db" 文件的第一页，特殊管理第一页
 * ValidCheck：db 启动时给 100~107 字节处填入一个随机字节，db 关闭时将其拷贝到 108~115 字节（一共 8 个字节）
 * 数据库在每次启动时，就会检查第一页两处的字节是否相同，以此来判断上一次是否正常关闭。如果是异常关闭，就需要执行数据的恢复流程。
 */
public class PageOne {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC + 2*LEN_VC));
    }
}
