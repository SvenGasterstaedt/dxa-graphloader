/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxgraphloader;

import de.hhu.bsinfo.dxgraphloader.loader.GraphLoader;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * <h1>GraphLoaderApp</h1>
 * The GraphLoaderApp extends an AbstractApplication and can be run on
 * on auto start or via the DxRam-Terminal.
 *
 * @author Sven Gasterstaedt
 * @version 1.0
 * @since 2019-03-15
 */


public class GraphLoaderApp extends AbstractApplication {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

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
        final BootService bootService = getService(BootService.class);
        final JobService jobService = getService(JobService.class);
        final ChunkService chunkService = getService(ChunkService.class);
        final SynchronizationService synchronizationService = getService(SynchronizationService.class);
        final NameserviceService nameserviceService = getService(NameserviceService.class);

        final GraphLoader graphLoader = new GraphLoader(bootService, jobService, chunkService,nameserviceService,synchronizationService);


        if (p_args.length < 3) {
            LOGGER.error("Parameters required! <int numWorkers> <boolean parseWhileReading> <String format> <Stringpath files>\nTerminated!");
            return;
        }
        int numWorkers;
        try {
            numWorkers = Integer.parseInt(p_args[0]);
        }catch(Exception e){
            LOGGER.error("Couldn't parse args!");
            return;
        }
        String format = p_args[1].toUpperCase();
        String[] file_paths = Arrays.copyOfRange(p_args, 2, p_args.length);
        if (!graphLoader.supportedFormats.isSupported(format)) {
            LOGGER.error(format + " is no not supported!");
            LOGGER.info("List of supported formats:");
            for (String f : graphLoader.supportedFormats.supportedFormats()) {
                LOGGER.info(f);
            }
            return;
        }

        for (String file : file_paths) {
            if (!Files.isRegularFile(Paths.get(file))) {
                LOGGER.error(file + " is no regular file!");
                return;
            }
        }
        //prints id of the distrubuted object table
        long l = graphLoader.loadFormat(format,file_paths,numWorkers).getID();
        try {
            sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(Long.toHexString(l));
    }

    @Override
    public void signalShutdown() {
    }
}

