package de.hhu.bsinfo.dxapp;

import de.hhu.bsinfo.dxapp.data.Vertex;
import de.hhu.bsinfo.dxapp.parser.GraphFormatManager;
import de.hhu.bsinfo.dxapp.parser.GraphFormatParser;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * "GraphLoaderApp"
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public class GraphLoaderApp extends AbstractApplication {

    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractApplication.class.getSimpleName());
    private RandomAccessFile file;


    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "GraphLoaderApp";
    }

    @Override
    public void main(final String[] p_args) {
        BootService bootService = getService(BootService.class);
        ChunkService chunkService = getService(ChunkService.class);

        List<Short> peers = bootService.getOnlinePeerNodeIDs();
        int peer_count = peers.size();

        if (p_args.length < 2) {
            LOGGER.error("Parameters required! <filepath> <format> [optional:peer_count]\nTerminated!");
            return;
        }
        String file_path = p_args[0];
        String format = p_args[1];
        try {
            if (Files.isRegularFile(Paths.get(file_path))) {
                LOGGER.info("Found file: " + file_path);

                file = new RandomAccessFile(new File(file_path), "r");

            } else {
                LOGGER.error("File not found: " + file_path + "\nTerminated!");
                return;
            }

            long approx_chunk_size = file.length() / peer_count;

            LOGGER.debug("Chunk Size: " + (approx_chunk_size / (1024)) + "KB");

            byte[] chunk = new byte[0];

            GraphFormatManager formatManager = new GraphFormatManager();
            GraphFormatParser g = formatManager.getFormat("edgelist",chunk);
            while(g.ready()){
                for(Vertex v : g.getVertices()){
                    chunkService.create().create((short)1, v);
                    chunkService.put().put(v);
                }
            }


        } catch (Exception ex) {
            LOGGER.trace(ex.getMessage() + "\nTerminated!");
            return;
        }
    }


    @Override
    public void signalShutdown() {
        // Interrupt any flow of your application and make sure it shuts down.
        // Do not block here or wait for something to shut down. Shutting down of your application
        // must be execute asynchronously
        System.out.println("Got killed!");
    }
}
