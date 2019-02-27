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

package de.hhu.bsinfo.dxgraphloader.formats;

import de.hhu.bsinfo.dxgraphloader.formats.readers.EdgeListReader;
import de.hhu.bsinfo.dxgraphloader.formats.splitter.JSONGraphFileChunkCreator;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormat;

public class JSONGraph extends GraphFormat {
    public JSONGraph(final String... files) {
        super(files);

        CYCLES = 2;
        //key format properties
        this.setFileChunkCreator(new JSONGraphFileChunkCreator(files[0], 16777216));
        this.setGraphFormatReader(EdgeListReader.class);
    }
}
