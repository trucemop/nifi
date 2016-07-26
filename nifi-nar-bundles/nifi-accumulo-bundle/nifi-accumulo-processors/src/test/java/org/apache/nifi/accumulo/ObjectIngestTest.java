package org.apache.nifi.accumulo;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import static org.junit.Assert.*;

public class ObjectIngestTest {

    @Test
    public void testGetDirList() {
        String path = "/home/user/file.txt";

        String pathSep = "/";
        List<String> dirList = ObjectIngest.getDirList(path, pathSep);

        assertEquals("Entry should be equal", "", dirList.get(0));
        assertEquals("Entry should be equal", "/home", dirList.get(1));
        assertEquals("Entry should be equal", "/home/user", dirList.get(2));
        assertEquals("Number of entries should be equal", 3, dirList.size());
    }

    @Test
    public void testInsertObjectDataZeroChunks() throws Exception {
        Instance instance = new MockInstance();

        Connector connector = instance.getConnector("root", new PasswordToken(""));
        connector.tableOperations().create("table");

        BatchWriter bw = connector.createBatchWriter("table", new BatchWriterConfig());

        String objectIdValue = "ABCDEFG";
        String objectIdKey = "objectHash";
        String objectNameIdValue = "HIJKLMNOP";
        String objectNameIdKey = "objectNameHash";

        String content = "Mary had a little lamb.";

        String pathSep = "/";

        long timestamp = 1234L;
        int chunkSize = 0;
        byte[] chunkSizeBytes = ByteBuffer.allocate(Integer.BYTES).putInt(chunkSize).array();

        InputStream stream = new ByteArrayInputStream(content.getBytes());

        Map<String, String> refMap = new HashMap<>();
        refMap.put(objectIdKey, objectIdValue);
        refMap.put(objectNameIdKey, objectNameIdValue);

        ColumnVisibility cv = new ColumnVisibility("group");

        ObjectIngest ingest = new ObjectIngest(chunkSize, pathSep, cv);

        ingest.insertObjectData(objectIdKey, objectNameIdKey, timestamp, refMap, stream, bw);

        Scanner scanner = connector.createScanner("table", new Authorizations("group"));

        List<Map.Entry<Key, Value>> entryList = new ArrayList<>();

        for (Map.Entry<Key, Value> entry : scanner) {
            entryList.add(entry);
        }


        assertEquals("Number of mutations should be equal", 0, entryList.size());
    }

    @Test
    public void testInsertObjectDataOneChunk() throws Exception {
        Instance instance = new MockInstance();

        Connector connector = instance.getConnector("root", new PasswordToken(""));
        connector.tableOperations().create("table");

        BatchWriter bw = connector.createBatchWriter("table", new BatchWriterConfig());

        String objectIdValue = "ABCDEFG";
        String objectIdKey = "objectHash";
        String objectNameIdValue = "HIJKLMNOP";
        String objectNameIdKey = "objectNameHash";

        String content = "Mary had a little lamb.";

        String pathSep = "/";

        long timestamp = 1234L;
        int chunkSize = 100000;
        byte[] chunkSizeBytes = ByteBuffer.allocate(Integer.BYTES).putInt(chunkSize).array();

        InputStream stream = new ByteArrayInputStream(content.getBytes());

        Map<String, String> refMap = new HashMap<>();
        refMap.put(objectIdKey, objectIdValue);
        refMap.put(objectNameIdKey, objectNameIdValue);

        ColumnVisibility cv = new ColumnVisibility("group");

        ObjectIngest ingest = new ObjectIngest(chunkSize, pathSep, cv);

        ingest.insertObjectData(objectIdKey, objectNameIdKey, timestamp, refMap, stream, bw);

        Scanner scanner = connector.createScanner("table", new Authorizations("group"));

        List<Map.Entry<Key, Value>> entryList = new ArrayList<>();

        for (Map.Entry<Key, Value> entry : scanner) {
            entryList.add(entry);
        }

        KeyValue first = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF, 
                KeyUtil.buildNullSepText(objectNameIdValue, objectIdKey), cv, timestamp),
                new Value(objectIdValue.getBytes()));

        KeyValue second = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF, 
                KeyUtil.buildNullSepText(objectNameIdValue, objectNameIdKey), cv, timestamp),
                new Value(objectNameIdValue.getBytes()));
        
        Text chunkCq = new Text(chunkSizeBytes);
        
        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(0).array(),0,Integer.BYTES);
        
        KeyValue third = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.CHUNK_CF, 
                chunkCq, cv, timestamp),
                new Value(content.getBytes()));
        
        chunkCq = new Text(chunkSizeBytes);
        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(),0,Integer.BYTES);
        
        KeyValue fourth = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.CHUNK_CF, 
                chunkCq, cv, timestamp),
                ObjectIngest.NULL_VALUE);
        
        
        assertEquals("First entry should be equal", first, entryList.get(0));
        assertEquals("Second entry should be equal", second, entryList.get(1));
        assertEquals("Third entry should be equal", third, entryList.get(2));
        assertEquals("Fourth entry should be equal", fourth, entryList.get(3));

        assertEquals("Number of mutations should be equal", 4, entryList.size());
    }
    
    @Test
    public void testInsertObjectDataMultipleChunks() throws Exception {
        Instance instance = new MockInstance();

        Connector connector = instance.getConnector("root", new PasswordToken(""));
        connector.tableOperations().create("table");

        BatchWriter bw = connector.createBatchWriter("table", new BatchWriterConfig());

        String objectIdValue = "ABCDEFG";
        String objectIdKey = "objectHash";
        String objectNameIdValue = "HIJKLMNOP";
        String objectNameIdKey = "objectNameHash";

        String content = "Mary had a little lamb.";

        String pathSep = "/";

        long timestamp = 1234L;
        int chunkSize = 10;
        byte[] chunkSizeBytes = ByteBuffer.allocate(Integer.BYTES).putInt(chunkSize).array();

        InputStream stream = new ByteArrayInputStream(content.getBytes());

        Map<String, String> refMap = new HashMap<>();
        refMap.put(objectIdKey, objectIdValue);
        refMap.put(objectNameIdKey, objectNameIdValue);

        ColumnVisibility cv = new ColumnVisibility("group");

        ObjectIngest ingest = new ObjectIngest(chunkSize, pathSep, cv);

        ingest.insertObjectData(objectIdKey, objectNameIdKey, timestamp, refMap, stream, bw);

        Scanner scanner = connector.createScanner("table", new Authorizations("group"));

        List<Map.Entry<Key, Value>> entryList = new ArrayList<>();

        for (Map.Entry<Key, Value> entry : scanner) {
            entryList.add(entry);
        }

        KeyValue first = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF, 
                KeyUtil.buildNullSepText(objectNameIdValue, objectIdKey), cv, timestamp),
                new Value(objectIdValue.getBytes()));

        KeyValue second = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.REFS_CF, 
                KeyUtil.buildNullSepText(objectNameIdValue, objectNameIdKey), cv, timestamp),
                new Value(objectNameIdValue.getBytes()));
        
        Text chunkCq = new Text(chunkSizeBytes);
        
        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(0).array(),0,Integer.BYTES);
        
        KeyValue third = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.CHUNK_CF, 
                chunkCq, cv, timestamp),
                new Value("Mary had a".getBytes()));
        
        chunkCq = new Text(chunkSizeBytes);
        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(),0,Integer.BYTES);
        
        KeyValue fourth = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.CHUNK_CF, 
                chunkCq, cv, timestamp),
                new Value(" little la".getBytes()));
        

        chunkCq = new Text(chunkSizeBytes);
        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(2).array(),0,Integer.BYTES);
        
        KeyValue fifth = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.CHUNK_CF, 
                chunkCq, cv, timestamp),
                new Value("mb.".getBytes()));        
        
        chunkCq = new Text(chunkSizeBytes);
        chunkCq.append(ByteBuffer.allocate(Integer.BYTES).putInt(3).array(),0,Integer.BYTES);
        
        KeyValue sixth = new KeyValue(new Key(new Text(objectIdValue), ObjectIngest.CHUNK_CF, 
                chunkCq, cv, timestamp),
                ObjectIngest.NULL_VALUE);  
        
        assertEquals("First entry should be equal", first, entryList.get(0));
        assertEquals("Second entry should be equal", second, entryList.get(1));
        assertEquals("Third entry should be equal", third, entryList.get(2));
        assertEquals("Fourth entry should be equal", fourth, entryList.get(3));
        assertEquals("Fifth entry should be equal", fifth, entryList.get(4));
        assertEquals("Sixth entry should be equal", sixth, entryList.get(5));
        assertEquals("Number of mutations should be equal", 6, entryList.size());
    }

    @Test
    public void testBuildDirectoryMutations() throws Exception {

        String path = "/home/user/file.txt";
        String pathName = "file";
        String pathSep = "/";
        String key = "key";
        String value = "value";
        long timestamp = 1234L;

        Map<String, String> refMap = new HashMap<>();
        refMap.put(pathName, path);
        refMap.put(key, value);

        ColumnVisibility cv = new ColumnVisibility("group");

        ObjectIngest ingest = new ObjectIngest(0, pathSep, cv);

        List<Mutation> mutList = ingest.buildDirectoryMutations(pathName, timestamp, refMap);

        Mutation first = new Mutation("000");
        first.put(ObjectIngest.DIR_COLF, ObjectIngest.TIME_TEXT, cv, timestamp, new Value(Long.toString(timestamp).getBytes()));

        Mutation second = new Mutation("001/home");
        second.put(ObjectIngest.DIR_COLF, ObjectIngest.TIME_TEXT, cv, timestamp, new Value(Long.toString(timestamp).getBytes()));

        Mutation third = new Mutation("002/home/user");
        third.put(ObjectIngest.DIR_COLF, ObjectIngest.TIME_TEXT, cv, timestamp, new Value(Long.toString(timestamp).getBytes()));

        byte[] timeBytes = ByteBuffer.allocate(8).putLong(Long.MAX_VALUE - timestamp).array();

        Mutation fourth = new Mutation("003/home/user/file.txt");
        fourth.put(new Text(timeBytes), new Text(pathName), cv, timestamp, new Value(path.getBytes()));
        fourth.put(new Text(timeBytes), new Text(key), cv, timestamp, new Value(value.getBytes()));

        assertEquals("First entry should be equal", first, mutList.get(0));
        assertEquals("Second entry should be equal", second, mutList.get(1));
        assertEquals("Third entry should be equal", third, mutList.get(2));
        assertEquals("Fourth entry should be equal", fourth, mutList.get(3));
        assertEquals("Number of mutations should be equal", 4, mutList.size());
    }

    @Test
    public void testBuildIndexMutation() throws Exception {
        String path = "/home/user/file.txt";
        String pathName = "file";
        String pathSep = "/";

        long timestamp = 1234L;

        Map<String, String> refMap = new HashMap<>();
        refMap.put(pathName, path);

        ColumnVisibility cv = new ColumnVisibility("group");

        ObjectIngest ingest = new ObjectIngest(0, pathSep, cv);

        List<Mutation> mutList = ingest.buildIndexMutations(pathName, timestamp, refMap);

        Mutation first = new Mutation("ffile.txt");
        first.put(ObjectIngest.INDEX_COLF, new Text("003" + path), cv, timestamp, ObjectIngest.NULL_VALUE);

        Mutation second = new Mutation("rtxt.elif");
        second.put(ObjectIngest.INDEX_COLF, new Text("003" + path), cv, timestamp, ObjectIngest.NULL_VALUE);

        assertEquals("First entry should be equal", first, mutList.get(0));
        assertEquals("Second entry should be equal", second, mutList.get(1));

        assertEquals("Number of mutations should be equal", 2, mutList.size());
    }

    @Test
    public void testBuildIndexMutationPathEnd() throws Exception {
        String path = "/home/user/";
        String pathName = "file";
        String pathSep = "/";

        long timestamp = 1234L;

        Map<String, String> refMap = new HashMap<>();
        refMap.put(pathName, path);

        ColumnVisibility cv = new ColumnVisibility("group");

        ObjectIngest ingest = new ObjectIngest(0, pathSep, cv);

        List<Mutation> mutList = ingest.buildIndexMutations(pathName, timestamp, refMap);

        assertEquals("Number of mutations should be equal", 0, mutList.size());
    }

    @Test
    public void testGetDepth() {

        String path = "/home/user/file.txt";

        String pathSep = "/";
        int depth = ObjectIngest.getDepth(path, pathSep);

        assertEquals("Depth should be equal", 3, depth);
    }

    @Test
    public void testGetRow() {
        String path = "/home/user/file.txt";

        String pathSep = "/";

        Text row = ObjectIngest.getRow(path, pathSep);
        Text rowSample = new Text("003" + path);

        assertEquals("Row should be equal", rowSample, row);
    }

    @Test
    public void testGetForwardIndex() {
        String path = "/home/user/file.txt";

        String pathSep = "/";
        Text row = ObjectIngest.getForwardIndex(path, pathSep);

        Text rowSample = new Text(ObjectIngest.FORWARD_PREFIX + "file.txt");

        assertEquals("Row should be equal", rowSample, row);
    }

    @Test
    public void testGetForwardIndexPathEnd() {
        String path = "/home/user/";

        String pathSep = "/";
        Text row = ObjectIngest.getForwardIndex(path, pathSep);

        assertNull("Row should be null", row);
    }

    @Test
    public void testGetReverseIndex() {
        String path = "/home/user/file.txt";

        String pathSep = "/";
        Text row = ObjectIngest.getReverseIndex(path, pathSep);

        Text rowSample = new Text(ObjectIngest.REVERSE_PREFIX + "txt.elif");

        assertEquals("Row should be equal", rowSample, row);
    }

    @Test
    public void testIntToBytes() {
    }

}
