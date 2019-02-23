package de.hhu.bsinfo.dxgraphloader.loader;

import de.hhu.bsinfo.dxgraphloader.loader.data.PeerVertexMap;
import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxram.job.AbstractJob;

import java.util.concurrent.locks.Lock;

public class SyncChunkJob extends AbstractJob {

    private Lock lock;

    public SyncChunkJob(Vertex[] localVertex, final PeerVertexMap peerVertexMap) {
        //peerVertexMap.putIfAbsent(localVertex)
    }

    @Override
    public void execute() {

    }
}
