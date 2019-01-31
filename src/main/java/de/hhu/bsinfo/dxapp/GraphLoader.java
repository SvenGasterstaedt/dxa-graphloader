package de.hhu.bsinfo.dxapp;

import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.net.NetworkService;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * "GraphLoader"
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public class GraphLoader extends AbstractApplication {

    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractApplication.class.getSimpleName());
    private List<Short> peers;
    private int peer_count;
    private RandomAccessFile file;


    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "GraphLoader";
    }

    @Override
    public void main(final String[] p_args) {
        BootService bootService = getService(BootService.class);
        NetworkService networkService = getService(NetworkService.class);

        //Parameter überprüfung
        if (p_args.length < 2) {
            LOGGER.error("Parameters required! <filepath> <format> [optional:peer_count]");
            LOGGER.error("Terminated!");
            return;
        }
        String file_path = p_args[0];
        String format = p_args[1];
        try {
            if (Files.isRegularFile(Paths.get(file_path))) {
                LOGGER.info("Found file: " + file_path);
                file = new RandomAccessFile(new File(file_path), "r");
            } else {
                LOGGER.error("File not found: " + file_path);
                LOGGER.error("Terminated!");
                return;
            }

            peers = bootService.getOnlinePeerNodeIDs();
            peer_count = peers.size();

            long aprox_chunk_size = file.length() / peer_count;
            //networkService.sendMessage();
            LOGGER.debug("Chunk Size: " + (aprox_chunk_size/(1024)) + "KB");

            for (short p : peers) {
                System.out.println(bootService.getNodeRole(p).toString());
            }
            System.out.println("Total peers: " + peer_count);

            JSONParser jsonParser = new JSONParser("formats.json");

        } catch (IOException ioex) {
            LOGGER.trace(ioex.getMessage());
            LOGGER.error("Terminated!");
            return;
        }

        try {
            Class exampleClass = Class.forName(format);
            Object ob = exampleClass.newInstance();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();

        } catch (Exception e) {
            e.printStackTrace();
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
