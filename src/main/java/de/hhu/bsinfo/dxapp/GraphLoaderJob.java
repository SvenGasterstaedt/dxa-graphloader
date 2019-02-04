package de.hhu.bsinfo.dxapp;

import de.hhu.bsinfo.dxapp.formats.GraphFormat;
import de.hhu.bsinfo.dxapp.formats.SupportedFormats;
import de.hhu.bsinfo.dxapp.formats.split.FileChunkCreator;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class GraphLoaderJob extends AbstractJob {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());


    String format;
    String[] files;
    List<Short> peers;

    public GraphLoaderJob(final String format,final String[] files,final List<Short> peers) throws Exception {
        super();
        this.format = format.toUpperCase();
        this.files = files;
        this.peers = peers;

        if (!SupportedFormats.formats.containsKey(this.format)) {
            LOGGER.error(this.format + " is no not supported!");
            LOGGER.info("List of supported formats:");
            SupportedFormats.formats.forEach(LOGGER::info);
            throw new Exception("GraphLoader terminated!");
        }

        for (String file : files) {
            if (!Files.isRegularFile(Paths.get(file))) {
                LOGGER.error(file + " is no regular file!");
                throw new Exception("GraphLoader terminated!");
            }
        }
    }

    @Override
    public short getTypeID() {
        return 1;
    }

    @Override
    public void execute() {
        try {
            GraphFormat graphFormat = SupportedFormats.formats.get(format).getConstructor().newInstance((String[]) files);
            FileChunkCreator chunkCreator = graphFormat.getFileChunkCreator();

            while(chunkCreator.hasRemaining()){
                chunkCreator.getNextChunk();
            }

            //distrubute file chunks onto peers
            //start remote job

            ChunkService chunkService = getService(ChunkService.class);

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    public boolean didSucceed() {
        return true;
    }


    @Override
    public void exportObject(final Exporter exporter) {
        exporter.writeString(format);
        exporter.writeInt(files.length);
        for (String file : files)
            exporter.writeString(file);
        exporter.writeInt(peers.size());
        for (short p : peers)
            exporter.writeShort(p);
    }

    @Override
    public void importObject(final Importer importer) {
        int n = 0;
        short j = 0;

        importer.readString(format);
        n = importer.readInt(n);
        files = new String[n];
        for (int i = 0; i < n; i++) {
            importer.readString(files[i]);
        }
        n = importer.readInt(n);
        peers = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            peers.add(importer.readShort(j));
        }
    }

    @Override
    public int sizeofObject() {
        int sizeoffiles = 0;
        for (int i = 0; i < files.length; i++)
            sizeoffiles += ObjectSizeUtil.sizeofString(files[i]);
        return ObjectSizeUtil.sizeofString(format) + Integer.BYTES + sizeoffiles + Integer.BYTES + peers.size() * Short.BYTES;
    }
}
