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
package org.apache.nifi.accumulo;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import static org.junit.Assert.assertEquals;
import org.junit.Before;

public class TestPutAccumuloObject {

    private String objectTable;
    private String directoryTable;
    private String indexTable;
    private String userName;
    private String instanceName;
    private String password;
    private String visibilityValue;
    private String visibilityKey;
    private String objectIdValue;
    private String objectIdKey;
    private String objectNameIdValue;
    private String objectNameIdKey;
    private String objectNameValue;
    private String objectNameKey;
    private String timestampKey;
    private long timestamp;
    private int chunkSize;
    private byte[] chunkSizeBytes;
    private String blacklist;
    private String content;
    private TestRunner runner;
    private PutAccumuloObject pao;
    private List<KeyValue> objectTableResults;
    private List<KeyValue> dirTableResults;
    private List<KeyValue> indexTableResults;

    @Before
    public void setup() {
        objectTable = "object";
        directoryTable = "dir";
        indexTable = "index";
        userName = "root";
        instanceName = "instance";
        password = "";
        visibilityValue = "group";
        visibilityKey = "vis";
        objectIdValue = "ABCDEFG";
        objectIdKey = "objectHash";
        objectNameIdValue = "HIJKLMNOP";
        objectNameIdKey = "objectNameHash";
        objectNameValue = "/home/user/file.txt";
        objectNameKey = "filename";
        timestampKey = "timestamp";
        timestamp = 1234L;
        chunkSize = 100000;
        chunkSizeBytes = ByteBuffer.allocate(Integer.BYTES).putInt(chunkSize).array();
        blacklist = "uuid,path,";
        content = "Mary had a little lamb.";
        runner = TestRunners.newTestRunner(PutAccumuloObject.class);
        pao = (PutAccumuloObject) runner.getProcessor();
        objectTableResults = new ArrayList<>();
        dirTableResults = new ArrayList<>();
        indexTableResults = new ArrayList<>();
    }

    private void buildDefaultTest() throws AccumuloException, AccumuloSecurityException, TableExistsException, UnsupportedEncodingException {
        runner.setProperty(PutAccumuloObject.INSTANCE_NAME, instanceName);
        runner.setProperty(PutAccumuloObject.USERNAME, userName);
        runner.setProperty(PutAccumuloObject.PASSWORD, password);
        runner.setProperty(PutAccumuloObject.OBJECT_TABLE, objectTable);
        runner.setProperty(PutAccumuloObject.DIRECTORY_TABLE, directoryTable);
        runner.setProperty(PutAccumuloObject.INDEX_TABLE, indexTable);
        runner.setProperty(PutAccumuloObject.VISIBILITY_ATTRIBUTE, visibilityKey);
        runner.setProperty(PutAccumuloObject.CHUNK_SIZE, Integer.toString(chunkSize));
        runner.setProperty(PutAccumuloObject.OBJECT_NAME_ATTRIBUTE, objectNameKey);
        runner.setProperty(PutAccumuloObject.OBJECT_ID_ATTRIBUTE, objectIdKey);
        runner.setProperty(PutAccumuloObject.OBJECT_NAME_ID_ATTRIBUTE, objectNameIdKey);
        runner.setProperty(PutAccumuloObject.TIMESTAMP_ATTRIBUTE, timestampKey);
        runner.setProperty(PutAccumuloObject.BLACKLIST, blacklist + visibilityKey);
        Instance instance = new MockInstance();

        pao.connector = instance.getConnector(userName, new PasswordToken(password));
        pao.connector.tableOperations().create(objectTable);
        pao.connector.tableOperations().create(directoryTable);
        pao.connector.tableOperations().create(indexTable);

        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put(objectIdKey, objectIdValue);
        attributes.put(objectNameIdKey, objectNameIdValue);
        attributes.put(objectNameKey, objectNameValue);
        attributes.put(timestampKey, Long.toString(timestamp));
        attributes.put(visibilityKey, visibilityValue);
        runner.enqueue(content.getBytes("UTF-8"), attributes);

        ColumnVisibility cv = new ColumnVisibility(visibilityValue);

        objectTableResults.add(new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue, objectNameKey), cv, timestamp),
                new Value(objectNameValue.getBytes())));

        objectTableResults.add(new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue, objectIdKey), cv, timestamp),
                new Value(objectIdValue.getBytes())));

        objectTableResults.add(new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue, objectNameIdKey), cv, timestamp),
                new Value(objectNameIdValue.getBytes())));

        objectTableResults.add(new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue, timestampKey), cv, timestamp),
                new Value(Long.toString(timestamp).getBytes())));

        Text chunkCq = new Text(chunkSizeBytes);

        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(0).array(), 0, Integer.BYTES);

        objectTableResults.add(new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.CHUNK_CF,
                chunkCq, cv, timestamp),
                new Value(content.getBytes())));

        chunkCq = new Text(chunkSizeBytes);
        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), 0, Integer.BYTES);

        objectTableResults.add(new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.CHUNK_CF,
                chunkCq, cv, timestamp),
                ObjectIngest.NULL_VALUE));

        dirTableResults.add(new KeyValue(new Key(new Text("000"), ObjectIngest.DIR_COLF, ObjectIngest.TIME_TEXT, cv, timestamp), new Value(Long.toString(timestamp).getBytes())));
        dirTableResults.add(new KeyValue(new Key(new Text("001/home"), ObjectIngest.DIR_COLF, ObjectIngest.TIME_TEXT, cv, timestamp), new Value(Long.toString(timestamp).getBytes())));
        dirTableResults.add(new KeyValue(new Key(new Text("002/home/user"), ObjectIngest.DIR_COLF, ObjectIngest.TIME_TEXT, cv, timestamp), new Value(Long.toString(timestamp).getBytes())));

        byte[] timeBytes = ByteBuffer.allocate(8).putLong(Long.MAX_VALUE - timestamp).array();

        dirTableResults.add(new KeyValue(new Key(new Text("003/home/user/file.txt"), new Text(timeBytes), new Text(objectNameKey), cv, timestamp), new Value(objectNameValue.getBytes())));
        dirTableResults.add(new KeyValue(new Key(new Text("003/home/user/file.txt"), new Text(timeBytes), new Text(objectIdKey), cv, timestamp), new Value(objectIdValue.getBytes())));
        dirTableResults.add(new KeyValue(new Key(new Text("003/home/user/file.txt"), new Text(timeBytes), new Text(objectNameIdKey), cv, timestamp), new Value(objectNameIdValue.getBytes())));
        dirTableResults.add(new KeyValue(new Key(new Text("003/home/user/file.txt"), new Text(timeBytes), new Text(timestampKey), cv, timestamp), new Value(Long.toString(timestamp).getBytes())));

        indexTableResults.add(new KeyValue(new Key(new Text("ffile.txt"), ObjectIngest.INDEX_COLF, new Text("003" + objectNameValue), cv, timestamp), ObjectIngest.NULL_VALUE));
        indexTableResults.add(new KeyValue(new Key(new Text("rtxt.elif"), ObjectIngest.INDEX_COLF, new Text("003" + objectNameValue), cv, timestamp), ObjectIngest.NULL_VALUE));
    }

    @Test
    public void testSingleFlowFile() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, UnsupportedEncodingException {

        buildDefaultTest();

        runner.run(1, false, false);

        Scanner objectScanner = pao.connector.createScanner(objectTable, new Authorizations(visibilityValue));

        List<Map.Entry<Key, Value>> objectEntryList = new ArrayList<>();

        for (Map.Entry<Key, Value> entry : objectScanner) {
            objectEntryList.add(entry);
        }

        assertEquals("Number of entries should be equal", 6, objectEntryList.size());
        assertEquals("First object entry should be equal", objectTableResults, objectEntryList);

        Scanner dirScanner = pao.connector.createScanner(directoryTable, new Authorizations(visibilityValue));

        List<Map.Entry<Key, Value>> dirEntryList = new ArrayList<>();

        for (Map.Entry<Key, Value> entry : dirScanner) {
            dirEntryList.add(entry);
        }

        assertEquals("Number of entries should be equal", 7, dirEntryList.size());
        assertEquals("Entries should be equal", dirTableResults, dirEntryList);

        Scanner indexScanner = pao.connector.createScanner(indexTable, new Authorizations(visibilityValue));

        List<Map.Entry<Key, Value>> indexEntryList = new ArrayList<>();

        for (Map.Entry<Key, Value> entry : indexScanner) {
            indexEntryList.add(entry);
        }

        assertEquals("Number of entries should be equal", 2, indexEntryList.size());
        assertEquals("Entries should be equal", indexTableResults, indexEntryList);

        assertEquals(1, runner.getProvenanceEvents().size());
        runner.assertAllFlowFilesTransferred(PutAccumuloObject.REL_SUCCESS);
    }

    @Test
    public void testMultipleFlowFiles() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, UnsupportedEncodingException {

        String visibilityValue2 = "group2";
        String objectIdValue2 = "qrstuv";
        String objectNameIdValue2 = "wxyz";
        String objectNameValue2 = "/home/user/file2.txt";
        String content2 = "No she didn't.";
        long timestamp2 = 5678L;

        buildDefaultTest();
        runner.setProperty(PutAccumuloObject.BATCH_SIZE, "2");

        Map<String, String> attributes2 = new LinkedHashMap<>();
        attributes2.put(objectIdKey, objectIdValue2);
        attributes2.put(objectNameIdKey, objectNameIdValue2);
        attributes2.put(objectNameKey, objectNameValue2);
        attributes2.put(timestampKey, Long.toString(timestamp2));
        attributes2.put(visibilityKey, visibilityValue2);
        runner.enqueue(content2.getBytes("UTF-8"), attributes2);

        runner.run(1, false, false);

        Scanner objectScanner = pao.connector.createScanner(objectTable, new Authorizations(visibilityValue, visibilityValue2));

        List<KeyValue> objectEntryList = new ArrayList<>();

        for (Map.Entry<Key, Value> entry : objectScanner) {
            objectEntryList.add(new KeyValue(entry.getKey(), entry.getValue()));
        }

        ColumnVisibility cv2 = new ColumnVisibility(visibilityValue2);

        objectTableResults.add(new KeyValue(new Key(new Text(objectIdValue2), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue2, objectNameKey), cv2, timestamp2),
                new Value(objectNameValue2.getBytes())));

        objectTableResults.add(new KeyValue(new Key(new Text(objectIdValue2), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue2, objectIdKey), cv2, timestamp2),
                new Value(objectIdValue2.getBytes())));

        objectTableResults.add(new KeyValue(new Key(new Text(objectIdValue2), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue2, objectNameIdKey), cv2, timestamp2),
                new Value(objectNameIdValue2.getBytes())));

        objectTableResults.add(new KeyValue(new Key(new Text(objectIdValue2), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue2, timestampKey), cv2, timestamp2),
                new Value(Long.toString(timestamp2).getBytes())));

        Text chunkCq = new Text(chunkSizeBytes);
        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(0).array(), 0, Integer.BYTES);

        objectTableResults.add(new KeyValue(new Key(new Text(objectIdValue2), ObjectIngest.CHUNK_CF,
                chunkCq, cv2, timestamp2),
                new Value(content2.getBytes())));

        chunkCq = new Text(chunkSizeBytes);
        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), 0, Integer.BYTES);

        objectTableResults.add(new KeyValue(new Key(new Text(objectIdValue2), ObjectIngest.CHUNK_CF,
                chunkCq, cv2, timestamp2),
                ObjectIngest.NULL_VALUE));

        assertEquals("Number of entries should be equal", 12, objectEntryList.size());
        assertEquals("Object entries should be equal", objectTableResults, objectEntryList);

        dirTableResults.add(new KeyValue(new Key(new Text("000"), ObjectIngest.DIR_COLF, ObjectIngest.TIME_TEXT, cv2, timestamp2), new Value(Long.toString(timestamp2).getBytes())));
        dirTableResults.add(new KeyValue(new Key(new Text("001/home"), ObjectIngest.DIR_COLF, ObjectIngest.TIME_TEXT, cv2, timestamp2), new Value(Long.toString(timestamp2).getBytes())));
        dirTableResults.add(new KeyValue(new Key(new Text("002/home/user"), ObjectIngest.DIR_COLF, ObjectIngest.TIME_TEXT, cv2, timestamp2), new Value(Long.toString(timestamp2).getBytes())));

        byte[] timeBytes = ByteBuffer.allocate(8).putLong(Long.MAX_VALUE - timestamp2).array();

        dirTableResults.add(new KeyValue(new Key(new Text("003/home/user/file2.txt"), new Text(timeBytes), new Text(objectNameKey), cv2, timestamp2), new Value(objectNameValue2.getBytes())));
        dirTableResults.add(new KeyValue(new Key(new Text("003/home/user/file2.txt"), new Text(timeBytes), new Text(objectIdKey), cv2, timestamp2), new Value(objectIdValue2.getBytes())));
        dirTableResults.add(new KeyValue(new Key(new Text("003/home/user/file2.txt"), new Text(timeBytes), new Text(objectNameIdKey), cv2, timestamp2), new Value(objectNameIdValue2.getBytes())));
        dirTableResults.add(new KeyValue(new Key(new Text("003/home/user/file2.txt"), new Text(timeBytes), new Text(timestampKey), cv2, timestamp2), new Value(Long.toString(timestamp2).getBytes())));

        Collections.sort(dirTableResults, new KeyValueComparator());

        Scanner dirScanner = pao.connector.createScanner(directoryTable, new Authorizations(visibilityValue, visibilityValue2));

        List<Map.Entry<Key, Value>> dirEntryList = new ArrayList<>();

        for (Map.Entry<Key, Value> entry : dirScanner) {
            dirEntryList.add(entry);
        }

        assertEquals("Number of entries should be equal", 14, dirEntryList.size());
        assertEquals("Entries should be equal", dirTableResults, dirEntryList);

        indexTableResults.add(new KeyValue(new Key(new Text("ffile2.txt"), ObjectIngest.INDEX_COLF, new Text("003" + objectNameValue2), cv2, timestamp2), ObjectIngest.NULL_VALUE));
        indexTableResults.add(new KeyValue(new Key(new Text("rtxt.2elif"), ObjectIngest.INDEX_COLF, new Text("003" + objectNameValue2), cv2, timestamp2), ObjectIngest.NULL_VALUE));

        Collections.sort(indexTableResults, new KeyValueComparator());

        Scanner indexScanner = pao.connector.createScanner(indexTable, new Authorizations(visibilityValue, visibilityValue2));

        List<Map.Entry<Key, Value>> indexEntryList = new ArrayList<>();

        for (Map.Entry<Key, Value> entry : indexScanner) {
            indexEntryList.add(entry);
        }

        assertEquals("Number of entries should be equal", 4, indexEntryList.size());
        assertEquals("Entries should be equal", indexTableResults, indexEntryList);

        assertEquals(2, runner.getProvenanceEvents().size());
        runner.assertAllFlowFilesTransferred(PutAccumuloObject.REL_SUCCESS);
    }

    private class KeyValueComparator implements Comparator<KeyValue> {

        @Override
        public int compare(KeyValue t, KeyValue t1) {
            return t.getKey().compareTo(t1.getKey());
        }

    }

}
