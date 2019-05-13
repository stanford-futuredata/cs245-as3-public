**Replicating gradescope tests locally**

The code can be compiled using Maven; the following command generates a jar file
for the entire project,

```bash
mvn package
```

You can simulate what the grader machines will do by running the autograder
script as follows:

```bash
java -Xmx1536m -Xms1536m -jar target/tests.jar 
```

This will output a json file that should match the graded tests run on
gradescope.

**Submitting your solution**

Run 
```bash
create_submission.sh
```

This will create a file student_submission.zip that you should upload to
gradescope.

Alternatively, just make a zip file containing **only** TransactionManager.java
with no directory structure and upload that.

**Teaching Staff Autograder Setup**

Create Autograder files and upload zip archive to Gradescope.

```bash
bash make_autograder.sh
```
