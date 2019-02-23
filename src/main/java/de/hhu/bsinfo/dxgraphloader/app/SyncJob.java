package de.hhu.bsinfo.dxgraphloader.app;

import de.hhu.bsinfo.dxgraphloader.app.data.PeerVertexMap;
import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxram.job.AbstractJob;

import java.util.concurrent.locks.Lock;

public class SyncJob extends AbstractJob {

    private Lock lock;

    public SyncJob(Vertex[] localVertex, final PeerVertexMap peerVertexMap) {
        //peerVertexMap.putIfAbsent(localVertex)
    }

    @Override
    public void execute() {

    }
}
