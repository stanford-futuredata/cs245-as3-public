package cs245.as3.driver;

import org.junit.Test;

import cs245.as3.interfaces.StorageManager.TaggedValue;

/**
 * DO NOT MODIFY THIS FILE IN THIS PACKAGE **
 */
public class StorageManagerTests {
    @Test
    public void TestSimple() {
    	StorageManagerImpl sm = new StorageManagerImpl();
        
    	long key = 10;
    	sm.blockPersistenceForKeys(new long[] {key});
    	
    	long tag = 100;
    	byte[] value = "John Doe".getBytes();
    	sm.queueWrite(key, tag, value);
    	TaggedValue tv = sm.readLatestTaggedValue(key);
    	assert(tv.tag == tag);
    	assert(tv.value == value);
    	
		//Manually call persistence once:
		sm.do_persistence_work();
    	tv = sm.readLatestTaggedValue(key);
    	assert(tv.tag == tag);
    	assert(tv.value == value);
		sm.crash();
		assert(sm.readLatestValue(key) == null);
		
		//Ensure that persistence does happen if we want it to:
    	sm.blockPersistenceForKeys(null);
    	sm.queueWrite(key, tag, value);
    	tv = sm.readLatestTaggedValue(key);
    	assert(tv.tag == tag);
    	assert(tv.value == value);
		//Manually call persistence:
		sm.do_persistence_work();
		sm.crash();
    	tv = sm.readLatestTaggedValue(key);
    	assert(tv.tag == tag);
    	assert(tv.value == value);
    }
}
