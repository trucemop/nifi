package org.apache.nifi.accumulo;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
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
import org.apache.nifi.processor.ProcessContext;
import org.junit.Test;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import static org.junit.Assert.*;

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
        runner.enqueue(content.getBytes("UTF-8"),attributes);

        runner.run(1,false, false);
        
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

//
//        assertEquals(1, runner.getProvenanceEvents().size());
        runner.assertAllFlowFilesTransferred(PutAccumuloObject.REL_SUCCESS);
    }

}
