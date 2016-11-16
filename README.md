Put a bit of benchmarking to CQEngine
* Prerequisites
  - Java8
  - Maven 3
* Use maven to build project
 ```
 mvn clean package
 ```
* Use java to run benchmark. Example:
```
 java -jar target\benchmarks.jar query.QueryTest -jvmArgs "-Xmx1300m -Xms1300m -XX:+UseG1GC"
```
* Test options:
 java -jar target\benchmarks.jar -h
* Results for my environment are[here](https://github.com/uujava/cqengine-query-jmh/releases/latest)

