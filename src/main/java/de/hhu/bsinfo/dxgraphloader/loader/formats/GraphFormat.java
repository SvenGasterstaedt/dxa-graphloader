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

package de.hhu.bsinfo.dxgraphloader.loader.formats;

public abstract class GraphFormat {
    FileChunkCreator fileChunkCreator;
    Class<? extends GraphFormatReader> formatReader;
    String[] files;

    public short CYCLES;

    public GraphFormat(final String... files) {
        this.files = files;
    }

    public FileChunkCreator getFileChunkCreator(short cycle) {
        fileChunkCreator.setCycle(cycle);
        return fileChunkCreator;
    }


    public Class<? extends GraphFormatReader> getGraphFormatReader() {
        return formatReader;
    }

    public void setGraphFormatReader(Class<? extends GraphFormatReader> formatReader){
        this.formatReader = formatReader;
    }

    public void setFileChunkCreator(FileChunkCreator fileChunkCreator){
        this.fileChunkCreator = fileChunkCreator;
    }
}


