package cs245.as3.driver;

import com.github.tkutche1.jgrade.Grader;
import com.github.tkutche1.jgrade.gradescope.GradescopeJsonObserver;

/**
  * DO NOT MODIFY THIS FILE IN THIS PACKAGE **
  */
public class As3Grader {
    static {
        //necessary to make assert() work in graded tests
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }
    
	public static void main(String[] args) {
		TransactionManagerTests.TEST_SEEDS = new long[] {1,2,3,4};
		InterleavedTransactionManagerTests.TEST_SEEDS = new long[] {5,6,7,8};

		Grader grader = new Grader();
		GradescopeJsonObserver observer = new GradescopeJsonObserver();
		grader.attachOutputObserver(observer);
		
		grader.runJUnitGradedTests(TransactionManagerTests.class);
		grader.runJUnitGradedTests(InterleavedTransactionManagerTests.class);
		
		grader.notifyOutputObservers();
		observer.setPrettyPrint(2);
		System.out.println(observer.toString());
	}

}
