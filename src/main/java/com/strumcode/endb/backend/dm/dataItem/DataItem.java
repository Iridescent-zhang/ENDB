package com.strumcode.endb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import com.strumcode.endb.backend.dm.DataManagerImpl;
import com.strumcode.endb.backend.dm.page.Page;
import com.strumcode.endb.backend.utils.Parser;
import com.strumcode.endb.backend.utils.Types;
import com.strumcode.endb.backend.common.SubArray;

import java.util.Arrays;

/**
 * DataItem 是 DM 层向上层提供的数据抽象。上层模块通过地址，向 DM 请求到对应的 DataItem，再获取到其中的数据。
 */
public interface DataItem {
    SubArray data();                                // // 该方法返回的形式是 SubArray，这个在页层面是数据共享的。并且返回的是这个 dataItem 的纯数据，不包含其他 dataItem 的格式，这个纯数据的格式就是 Entry 记录
    
    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }

    // 从页面的 offset(页中数据偏移) 处解析出 dataitem，从 offset 开始长为 length 的都属于 dataitem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset+ DataItemImpl.OF_SIZE, offset+ DataItemImpl.OF_DATA));
        short length = (short)(size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], pg, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}
