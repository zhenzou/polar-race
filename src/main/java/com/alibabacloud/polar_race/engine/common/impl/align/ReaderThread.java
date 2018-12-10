package com.alibabacloud.polar_race.engine.common.impl.align;


import com.alibabacloud.polar_race.engine.common.Constants;
import com.alibabacloud.polar_race.engine.common.util.IOUtils;
import com.alibabacloud.polar_race.engine.common.util.UnsafeUtils;
import com.sun.jna.ptr.PointerByReference;

/**
 * @since 0.1
 * create at 2018-12-08
 */
public class ReaderThread extends Thread {

    PointerByReference ref = IOUtils.newPointer(Constants.VALUE_SIZE);

    long peer = UnsafeUtils.getPointerPeer(ref.getValue());

    public ReaderThread(Runnable target) {
        super(target);
    }
}
