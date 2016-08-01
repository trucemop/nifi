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

public class TestPutAccumuloObject {

    @Test
    public void testSingleFlowFile() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, UnsupportedEncodingException {

        final TestRunner runner = TestRunners.newTestRunner(PutAccumuloObject.class);

        String objectTable = "object";
        String directoryTable = "dir";
        String indexTable = "index";
        String userName = "root";
        String instanceName = "instance";
        String password = "";
        String visibilityValue = "group";
        String visibilityKey = "vis";
        String objectIdValue = "ABCDEFG";
        String objectIdKey = "objectHash";
        String objectNameIdValue = "HIJKLMNOP";
        String objectNameIdKey = "objectNameHash";
        String objectNameValue = "/home/user/file.txt";
        String objectNameKey = "filename";
        String timestampKey = "timestamp";

        long timestamp = 1234L;
        int chunkSize = 100000;
        byte[] chunkSizeBytes = ByteBuffer.allocate(Integer.BYTES).putInt(chunkSize).array();

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
        runner.setProperty(PutAccumuloObject.BLACKLIST, "uuid,path," + visibilityKey);

        String content = "Mary had a little lamb.";

        PutAccumuloObject pao = (PutAccumuloObject) runner.getProcessor();
        Instance instance = new MockInstance();

        pao.connector = instance.getConnector(userName, new PasswordToken(""));
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

        runner.run(1, false, false);

        Scanner objectScanner = pao.connector.createScanner(objectTable, new Authorizations(visibilityValue));

        List<Map.Entry<Key, Value>> entryList = new ArrayList<>();

        for (Map.Entry<Key, Value> entry : objectScanner) {
            entryList.add(entry);
        }
        ColumnVisibility cv = new ColumnVisibility(visibilityValue);

        KeyValue first = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue, objectNameKey), cv, timestamp),
                new Value(objectNameValue.getBytes()));

        KeyValue second = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue, objectIdKey), cv, timestamp),
                new Value(objectIdValue.getBytes()));

        KeyValue third = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue, objectNameIdKey), cv, timestamp),
                new Value(objectNameIdValue.getBytes()));

        KeyValue fourth = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue, timestampKey), cv, timestamp),
                new Value(Long.toString(timestamp).getBytes()));

        Text chunkCq = new Text(chunkSizeBytes);

        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(0).array(), 0, Integer.BYTES);

        KeyValue fifth = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.CHUNK_CF,
                chunkCq, cv, timestamp),
                new Value(content.getBytes()));

        chunkCq = new Text(chunkSizeBytes);
        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), 0, Integer.BYTES);

        KeyValue sixth = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.CHUNK_CF,
                chunkCq, cv, timestamp),
                ObjectIngest.NULL_VALUE);

        assertEquals("First entry should be equal", first, entryList.get(0));
        assertEquals("Second entry should be equal", second, entryList.get(1));
        assertEquals("Third entry should be equal", third, entryList.get(2));
        assertEquals("Fourth entry should be equal", fourth, entryList.get(3));
        assertEquals("Fifth entry should be equal", fifth, entryList.get(4));
        assertEquals("Sixth entry should be equal", sixth, entryList.get(5));
        assertEquals("Number of entries should be equal", 6, entryList.size());

        assertEquals(1, runner.getProvenanceEvents().size());
        runner.assertAllFlowFilesTransferred(PutAccumuloObject.REL_SUCCESS);
    }

    @Test
    public void testMultipleFlowFiles() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, UnsupportedEncodingException {

        final TestRunner runner = TestRunners.newTestRunner(PutAccumuloObject.class);

        String objectTable = "object";
        String directoryTable = "dir";
        String indexTable = "index";
        String userName = "root";
        String instanceName = "instance";
        String password = "";
        String visibilityValue = "group";
        String visibilityKey = "vis";
        String objectIdValue = "ABCDEFG";
        String objectIdKey = "objectHash";
        String objectNameIdValue = "HIJKLMNOP";
        String objectNameIdKey = "objectNameHash";
        String objectNameValue = "/home/user/file.txt";
        String objectNameKey = "filename";
        String timestampKey = "timestamp";
        String content = "Mary had a little lamb.";
        long timestamp = 1234L;

        String visibilityValue2 = "group2";
        String objectIdValue2 = "qrstuv";
        String objectNameIdValue2 = "wxyz";
        String objectNameValue2 = "/home/user/file2.txt";
        String content2 = "No she didn't.";
        long timestamp2 = 5678L;

        int chunkSize = 100000;
        byte[] chunkSizeBytes = ByteBuffer.allocate(Integer.BYTES).putInt(chunkSize).array();

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
        runner.setProperty(PutAccumuloObject.BLACKLIST, "uuid,path," + visibilityKey);
        runner.setProperty(PutAccumuloObject.BATCH_SIZE, "2");

        PutAccumuloObject pao = (PutAccumuloObject) runner.getProcessor();
        Instance instance = new MockInstance();

        pao.connector = instance.getConnector(userName, new PasswordToken(""));
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

        Map<String, String> attributes2 = new LinkedHashMap<>();
        attributes2.put(objectIdKey, objectIdValue2);
        attributes2.put(objectNameIdKey, objectNameIdValue2);
        attributes2.put(objectNameKey, objectNameValue2);
        attributes2.put(timestampKey, Long.toString(timestamp2));
        attributes2.put(visibilityKey, visibilityValue2);
        runner.enqueue(content2.getBytes("UTF-8"), attributes2);

        runner.run(1, false, false);

        Scanner objectScanner = pao.connector.createScanner(objectTable, new Authorizations(visibilityValue, visibilityValue2));

        List<Map.Entry<Key, Value>> entryList = new ArrayList<>();

        for (Map.Entry<Key, Value> entry : objectScanner) {
            entryList.add(entry);
        }
        ColumnVisibility cv = new ColumnVisibility(visibilityValue);

        Text chunkCq = new Text(chunkSizeBytes);

        List<KeyValue> list = new ArrayList<>();

        list.add(new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue, objectNameKey), cv, timestamp),
                new Value(objectNameValue.getBytes())));

        list.add(new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue, objectIdKey), cv, timestamp),
                new Value(objectIdValue.getBytes())));

        list.add(new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue, objectNameIdKey), cv, timestamp),
                new Value(objectNameIdValue.getBytes())));

        list.add(new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue, timestampKey), cv, timestamp),
                new Value(Long.toString(timestamp).getBytes())));

        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(0).array(), 0, Integer.BYTES);

        list.add(new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.CHUNK_CF,
                chunkCq, cv, timestamp),
                new Value(content.getBytes())));

        chunkCq = new Text(chunkSizeBytes);
        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), 0, Integer.BYTES);

        list.add(new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.CHUNK_CF,
                chunkCq, cv, timestamp),
                ObjectIngest.NULL_VALUE));

        ColumnVisibility cv2 = new ColumnVisibility(visibilityValue2);

        list.add(new KeyValue(new Key(new Text(objectIdValue2), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue2, objectNameKey), cv2, timestamp2),
                new Value(objectNameValue2.getBytes())));

        list.add(new KeyValue(new Key(new Text(objectIdValue2), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue2, objectIdKey), cv2, timestamp2),
                new Value(objectIdValue2.getBytes())));

        list.add(new KeyValue(new Key(new Text(objectIdValue2), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue2, objectNameIdKey), cv2, timestamp2),
                new Value(objectNameIdValue2.getBytes())));

        list.add(new KeyValue(new Key(new Text(objectIdValue2), ObjectIngest.REFS_CF,
                ObjectIngest.buildNullSepText(objectNameIdValue2, timestampKey), cv2, timestamp2),
                new Value(Long.toString(timestamp2).getBytes())));

        chunkCq = new Text(chunkSizeBytes);
        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(0).array(), 0, Integer.BYTES);

        list.add(new KeyValue(new Key(new Text(objectIdValue2), ObjectIngest.CHUNK_CF,
                chunkCq, cv2, timestamp2),
                new Value(content2.getBytes())));

        chunkCq = new Text(chunkSizeBytes);
        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), 0, Integer.BYTES);

        list.add(new KeyValue(new Key(new Text(objectIdValue2), ObjectIngest.CHUNK_CF,
                chunkCq, cv2, timestamp2),
                ObjectIngest.NULL_VALUE));

        assertEquals("First entry should be equal", list.get(0), entryList.get(0));
        assertEquals("Second entry should be equal", list.get(1), entryList.get(1));
        assertEquals("Third entry should be equal", list.get(2), entryList.get(2));
        assertEquals("Fourth entry should be equal", list.get(3), entryList.get(3));
        assertEquals("Fifth entry should be equal", list.get(4), entryList.get(4));
        assertEquals("Sixth entry should be equal", list.get(5), entryList.get(5));

        assertEquals("Seventh entry should be equal", list.get(6), entryList.get(6));
        assertEquals("Eighth entry should be equal", list.get(7), entryList.get(7));
        assertEquals("Ninth entry should be equal", list.get(8), entryList.get(8));
        assertEquals("Tenth entry should be equal", list.get(9), entryList.get(9));
        assertEquals("Eleventh entry should be equal", list.get(10), entryList.get(10));
        assertEquals("Twelfth entry should be equal", list.get(11), entryList.get(11));

        assertEquals("Number of entries should be equal", 12, entryList.size());

        assertEquals(2, runner.getProvenanceEvents().size());
        runner.assertAllFlowFilesTransferred(PutAccumuloObject.REL_SUCCESS);
    }

}
