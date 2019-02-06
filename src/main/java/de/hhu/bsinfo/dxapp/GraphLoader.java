package de.hhu.bsinfo.dxapp;

import de.hhu.bsinfo.dxapp.data.FileChunk;
import de.hhu.bsinfo.dxapp.formats.GraphFormat;
import de.hhu.bsinfo.dxapp.formats.SupportedFormats;
import de.hhu.bsinfo.dxapp.formats.split.FileChunkCreator;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

class GraphLoaderJob {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

    private String format;
    private String[] files;
    private List<Short> peers;
    protected ChunkService chunkService;

    void setChunkService(final ChunkService chunkService) {
        this.chunkService = chunkService;
    }


    protected GraphLoaderJob(final String format, final String[] files, final List<Short> peers) throws Exception {
        super();
        this.format = format.toUpperCase();
        this.files = files;
        this.peers = peers;

        if (!SupportedFormats.isSupported(format)) {
            LOGGER.error(this.format + " is no not supported!");
            LOGGER.info("List of supported formats:");
            for (String f : SupportedFormats.supportedFormats()) {
                LOGGER.info(f);
            }
            throw new Exception("GraphLoader terminated!");
        }

        for (String file : files) {
            if (!Files.isRegularFile(Paths.get(file))) {
                LOGGER.error(file + " is no regular file!");
                throw new Exception("GraphLoader terminated!");
            }
        }
    }

    protected void execute() {

        GraphFormat graphFormat = SupportedFormats.getFormat(format, files);
        FileChunkCreator chunkCreator;
        if (graphFormat != null) {
            chunkCreator = graphFormat.getFileChunkCreator();
            while (chunkCreator.hasRemaining()) {
                for (short p : peers) {
                    long c_id = ChunkID.INVALID_ID;
                    FileChunk fileChunk = chunkCreator.getNextChunk();
                    chunkService.create().create(p, fileChunk);
                    chunkService.put().put(fileChunk);
                    System.out.println(Long.toHexString(fileChunk.getID()).toUpperCase());
                    if (!chunkCreator.hasRemaining()) {
                        break;
                    }
                }
            }
        }
    }
}
