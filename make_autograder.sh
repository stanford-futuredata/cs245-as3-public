#!/usr/bin/env bash
rm autograder.zip
zip -r autograder.zip src/ pom.xml setup.sh run_autograder -x src/cs245/as3/*.java
