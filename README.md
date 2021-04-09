# BigDataEx

# MUST INSTALL SUCCESS HADOOP AND MAVEN

# RUN BY FLOWING COMMANDS

Put dataset to hadoop file system
<code>
hdfs dfs -mkdir /input
</code>
<code>
  hdfs dfs -put data/retail.dat /input
</code>

In root of project build jar file with maven
<code>
mvn clean install
</code>

Jar file builded located in target folder
Run this command before hadoop started

<code>
hadoop jar target/lab-1.0-SNAPSHOT.jar lab3b.App /input /output
</code>

Get output to project
<code>
hdfs dfs -get /output output
</code>
