package org.apache.nifi.accumulo;

import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;


public class ObjectIngestTest {
    

    @Test
    public void testGetDirList() {
        String path = "/home/user/file.txt";
        
        List<String> dirList = ObjectIngest.getDirList(path, "/");
        
        
        int i = 0;
        
    }
    
}
