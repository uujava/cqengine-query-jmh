Put a bit of benchmarking to CQEngine
* Prerequisites
  - Java8
  - Maven 3
* use maven to build project
 mvn clean package
* use java to run benchmark. Example:
 java -jar target\benchmarks.jar QueryTest -jvmArgs "-Xmx1300m -Xms1300m -XX:+UseG1GC"
* test options:
 java -jar target\benchmarks.jar -h
* results for my environment [here](releases/latest)

