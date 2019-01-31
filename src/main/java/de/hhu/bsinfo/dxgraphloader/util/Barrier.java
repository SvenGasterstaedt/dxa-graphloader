package de.hhu.bsinfo.dxgraphloader.util;

import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;

public final class Barrier {
    private Barrier() {
    }

    public static int createBarrier(String p_lockname, int p_size, SynchronizationService p_sync,
            NameserviceService p_name) {
        int value = p_sync.barrierAllocate(p_size);
        p_sync.barrierGetStatus(value);
        p_name.register(value, p_lockname);
        return value;
    }

    public static int getBarrier(String p_lockname, NameserviceService p_name) {
        int value = BarrierID.INVALID_ID;
        while (value == BarrierID.INVALID_ID) {
            value = (int) p_name.getChunkID(p_lockname, 1000);
        }
        return value;
    }
}
