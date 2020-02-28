apt-get -y install openjdk-11-jdk-headless bash

# The following lines are to make sure we can install packages from maven
# See https://bugs.launchpad.net/ubuntu/+source/ca-certificates-java/+bug/1396760
# and https://github.com/docker-library/openjdk/issues/19#issuecomment-70546872
apt-get install --reinstall ca-certificates-java
update-ca-certificates -f

# Need maven 3.6 to suppress some warnings
cd /opt/
wget http://mirror.reverse.net/pub/apache/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
tar -xvzf apache-maven-3.6.3-bin.tar.gz
mv apache-maven-3.6.3 maven
