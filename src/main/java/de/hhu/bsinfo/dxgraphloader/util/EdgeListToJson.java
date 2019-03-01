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

package de.hhu.bsinfo.dxgraphloader.util;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;

import java.io.*;
import java.util.*;

public final class EdgeListToJson {

    public static void convert(String pathFrom, String pathTo) throws IOException {

        Set<String> nodes = new HashSet<>();
        ArrayList<String> source = new ArrayList<>();
        ArrayList<String> target = new ArrayList<>();


        File file = new File(pathFrom);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        while (reader.ready()) {
            String[] keys = reader.readLine().split("\t");
            if (keys[0].startsWith("#")) continue;
            nodes.add(keys[0]);
            nodes.add(keys[1]);
            source.add(keys[0]);
            target.add(keys[1]);
        }
        reader.close();


        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(pathTo)));
        JsonWriter jsonWriter = new Gson().newJsonWriter(writer);
        jsonWriter.beginObject();
        jsonWriter.name("graphs");
        jsonWriter.beginArray();
        jsonWriter.beginObject();
        jsonWriter.name("nodes");
        jsonWriter.beginArray();
        for (String key : nodes) {
            jsonWriter.beginObject();
            jsonWriter.name("id");
            jsonWriter.value(key);
            jsonWriter.endObject();
        }
        jsonWriter.endArray();
        jsonWriter.name("edges");
        jsonWriter.beginArray();
        for (int i = 0; i < source.size();i++){
            jsonWriter.beginObject();
            jsonWriter.name("source");
            jsonWriter.value(source.get(i));
            jsonWriter.name("target");
            jsonWriter.value(target.get(i));
            jsonWriter.endObject();
        }
        jsonWriter.endArray();
        jsonWriter.endObject();
        jsonWriter.endArray();
        jsonWriter.endObject();
        jsonWriter.close();
    }
}
