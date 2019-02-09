package de.hhu.bsinfo.dxapp.io;

public class Util {
    public static long fixChunkIDs(long id){
         return id << 56
                ^ id << 48 >>> 56 << 48
                ^ id << 40 >>> 56 << 40
                ^ id << 32 >>> 56 << 32
                ^ id << 24 >>> 56 << 24
                ^ id << 16 >>> 56 << 16
                ^ id << 8 >>> 56 << 8
                ^ id >>> 56;
    }
}
