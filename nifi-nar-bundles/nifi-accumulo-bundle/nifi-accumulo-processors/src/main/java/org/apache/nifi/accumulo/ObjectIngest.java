package org.apache.nifi.accumulo;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE object distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import java.util.Map;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;

public class ObjectIngest {

    public static final Text CHUNK_CF = new Text("~chunk");
    public static final Text REFS_CF = new Text("refs");
    public static final Text FORWARD_PREFIX = new Text("f");
    public static final Text REVERSE_PREFIX = new Text("r");
    public static final Text INDEX_COLF = new Text("i");
    public static final Text DIR_COLF = new Text("dir");
    public static final Text TIME_TEXT = new Text("time");

    public static final ByteSequence CHUNK_CF_BS = new ArrayByteSequence(CHUNK_CF.getBytes(), 0, CHUNK_CF.getLength());
    public static final ByteSequence REFS_CF_BS = new ArrayByteSequence(REFS_CF.getBytes(), 0, REFS_CF.getLength());
    static final Value NULL_VALUE = new Value(new byte[0]);

    public static final TypedValueCombiner.Encoder<Long> encoder = LongCombiner.FIXED_LEN_ENCODER;

    int chunkSize;
    String pathSep;
    byte[] chunkSizeBytes;
    byte[] buf;

    ColumnVisibility cv;

    public ObjectIngest(int chunkSize, String pathSep, ColumnVisibility colvis) {
        this.chunkSize = chunkSize;
        this.pathSep = pathSep;
        chunkSizeBytes = ByteBuffer.allocate(Integer.BYTES).putInt(chunkSize).array();
        buf = new byte[chunkSize];
        cv = colvis;
    }

    public void insertObjectData(String objectIdKey, String objectnameIdKey, long timestamp, Map<String, String> refMap, InputStream stream, BatchWriter bw) throws MutationsRejectedException, IOException {
        if (chunkSize == 0) {
            return;
        }

        String uid = refMap.get(objectnameIdKey);

        String hash = refMap.get(objectIdKey);
        Text row = new Text(hash);

        Mutation m = new Mutation(row);
        for (Map.Entry<String, String> entry : refMap.entrySet()) {
            m.put(REFS_CF, KeyUtil.buildNullSepText(uid, entry.getKey()), cv, timestamp, new Value(entry.getValue().getBytes()));
        }
        bw.addMutation(m);

        int chunkCount = 0;

        int numRead = stream.read(buf);
        while (numRead >= 0) {
            while (numRead < buf.length) {
                int moreRead = stream.read(buf, numRead, buf.length - numRead);
                if (moreRead > 0) {
                    numRead += moreRead;
                } else if (moreRead < 0) {
                    break;
                }
            }
            m = new Mutation(row);
            Text chunkCQ = new Text(chunkSizeBytes);
            chunkCQ.append(ByteBuffer.allocate(Integer.BYTES).putInt(chunkCount).array(), 0, Integer.BYTES);
            m.put(CHUNK_CF, chunkCQ, cv, timestamp, new Value(buf, 0, numRead));
            bw.addMutation(m);
            if (chunkCount == Integer.MAX_VALUE) {
                throw new RuntimeException("too many chunks for object " + uid + ", try raising chunk size");
            }
            chunkCount++;
            numRead = stream.read(buf);
        }

        m = new Mutation(row);
        Text chunkCQ = new Text(chunkSizeBytes);
        chunkCQ.append(ByteBuffer.allocate(Integer.BYTES).putInt(chunkCount).array(), 0, Integer.BYTES);
        m.put(new Text(CHUNK_CF), chunkCQ, cv, timestamp, new Value(new byte[0]));
        bw.addMutation(m);

    }

    public List<Mutation> buildDirectoryMutations(String objectnameKey, long timestamp, Map<String, String> refMap) {

        List<Mutation> list = new ArrayList<>();
        
        String name = refMap.get(objectnameKey);

        for (String dir : getDirList(name, pathSep)) {

            Mutation dirM = new Mutation(getRow(dir, pathSep));
            dirM.put(DIR_COLF, TIME_TEXT, cv, timestamp, new Value(Long.toString(timestamp).getBytes()));
            list.add(dirM);
        }

        Mutation m = new Mutation(getRow(name, pathSep));
        Text colf = new Text(encoder.encode(Long.MAX_VALUE - timestamp));
        for (Map.Entry<String, String> entry : refMap.entrySet()) {
            m.put(colf, new Text(entry.getKey()), cv, timestamp, new Value(entry.getValue().getBytes()));
        }
        list.add(m);

        return list;
    }

    public List<Mutation> buildIndexMutations(String objectnameKey, long timestamp, Map<String, String> refMap) {
        List<Mutation> list = new ArrayList<>();
        
        String path = refMap.get(objectnameKey);
        Text row = getForwardIndex(path, pathSep);
        if (row != null) {
            Text p = new Text(getRow(path, pathSep));
            Mutation m = new Mutation(row);
            m.put(INDEX_COLF, p, cv, timestamp, NULL_VALUE);
            list.add(m);

            row = getReverseIndex(path, pathSep);
            m = new Mutation(row);
            m.put(INDEX_COLF, p, cv, timestamp, NULL_VALUE);
            list.add(m);
        }
        return list;
    }

    public static List<String> getDirList(String path, String pathSep) {
        List<String> dirList = new ArrayList<>();

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        String[] paths = path.split(pathSep);
        for (int i = 0; i < paths.length - 1; i++) {
            String dir = paths[i];
            if (i != 0) {
                sb.append(pathSep);
            }
            sb.append(dir);

            dirList.add(sb.toString());
        }

        return dirList;
    }

    /**
     * Calculates the depth of a path, i.e. the number of separators in the path
     * name.
     *
     * @param path the full path of an object
     * @param pathSep separator to use for path entries
     * @return the depth of the path
     */
    public static int getDepth(String path, String pathSep) {
        int numSlashes = 0;
        int index = -1;
        while ((index = path.indexOf(pathSep, index + 1)) >= 0) {
            numSlashes++;
        }
        return numSlashes;
    }

    /**
     * Given a path, construct an accumulo row prepended with the path's depth
     * for the directory table.
     *
     * @param path the full path of a object
     * @param pathSep separator to use for path entries
     * @return the accumulo row associated with this path
     */
    public static Text getRow(String path, String pathSep) {
        Text row = new Text(String.format("%03d", getDepth(path, pathSep)));
        row.append(path.getBytes(), 0, path.length());
        return row;
    }

    /**
     * Given a path, construct an accumulo row prepended with the
     * {@link #FORWARD_PREFIX} for the index table.
     *
     * @param path the full path of a object
     * @param pathSep separator to use for path entries
     * @return the accumulo row associated with this path
     */
    public static Text getForwardIndex(String path, String pathSep) {
        String part = path.substring(path.lastIndexOf(pathSep) + 1);
        if (part.length() == 0) {
            return null;
        }
        Text row = new Text(FORWARD_PREFIX);
        row.append(part.getBytes(), 0, part.length());
        return row;
    }

    /**
     * Given a path, construct an accumulo row prepended with the
     * {@link #REVERSE_PREFIX} with the path reversed for the index table.
     *
     * @param path the full path of a object
     * @param pathSep separator to use for path entries
     * @return the accumulo row associated with this path
     */
    public static Text getReverseIndex(String path, String pathSep) {
        String part = path.substring(path.lastIndexOf(pathSep) + 1);
        if (part.length() == 0) {
            return null;
        }
        byte[] rev = new byte[part.length()];
        int i = part.length() - 1;
        for (byte b : part.getBytes()) {
            rev[i--] = b;
        }
        Text row = new Text(REVERSE_PREFIX);
        row.append(rev, 0, rev.length);
        return row;
    }


}
