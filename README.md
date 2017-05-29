# Barebones setup for Spark + Scala + Maven

```bash
mvn clean
mvn package
spark-submit --class com.test.scala.App --master local target/scalatest2-1.0-SNAPSHOT.jar AAPL.csv output
```

## IntelliJ
New Project > Maven > org.scala-tools.archetypes:scala-archetype-simple
