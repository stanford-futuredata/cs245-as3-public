package cs245.as3.driver;

import com.github.tkutche1.jgrade.Grader;
import com.github.tkutche1.jgrade.gradescope.GradescopeJsonObserver;
import org.json.JSONObject;
import org.json.JSONArray;

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
		LeaderboardTests.TEST_SEEDS = new long[] {9,10,11,12};

		Grader grader = new Grader();
		GradescopeJsonObserver observer = new GradescopeJsonObserver();
		grader.attachOutputObserver(observer);
		
		grader.runJUnitGradedTests(TransactionManagerTests.class);
		grader.runJUnitGradedTests(InterleavedTransactionManagerTests.class);

		grader.notifyOutputObservers();

		JSONObject json = observer.getJson();

		if (grader.allTestsPassed()) {
			// Only run leaderboard benchmark once all other tests are passing.
			int IOs = LeaderboardTests.TestWriteOps();

			JSONArray leaderboard = new JSONArray();
			JSONObject iosField = new JSONObject();
			iosField.put("name", "IOs");
			iosField.put("value", IOs);
			iosField.put("order", "asc");
			leaderboard.put(iosField);
			json.put("leaderboard", leaderboard);
		}

		System.out.println(json.toString(2));
	}

}
