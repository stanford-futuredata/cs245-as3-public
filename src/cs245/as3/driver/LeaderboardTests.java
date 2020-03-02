package cs245.as3.driver;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.github.tkutche1.jgrade.gradedtest.GradedTest;

import cs245.as3.TransactionManager;
import cs245.as3.driver.LogManagerImpl.CrashException;

/**
 * DO NOT MODIFY THIS FILE IN THIS PACKAGE **
 */
public class LeaderboardTests {

    //Test seeds will be modified by the autograder
    protected static long[] TEST_SEEDS = new long[] {0x12345671234567L, 0x1000, 42, 9};

    /**
     * Test that the transaction manager is doing something smart with log truncation
     */
    public static int TestWriteOps() {
        LogManagerImpl lm = new LogManagerImpl();
        StorageManagerImpl sm = new StorageManagerImpl();
        TransactionManager tm = new TransactionManager();
        sm.setPersistenceListener(tm);
        sm.in_recovery = true;
        tm.initAndRecover(sm, lm);
        sm.in_recovery = false;

        Random r = new Random(TEST_SEEDS[0]);

        for(int i = 0; i < 100; i++) {
            long TXid = i;
            tm.start(TXid);
            for (int j = 0; j < 500; j++) {
                tm.write(TXid, r.nextInt(100), String.format("%d",i).getBytes());
            }
            tm.commit(TXid);
        }

        return lm.getIOPCount();
    }
}
