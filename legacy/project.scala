// scala-cli build for the legacy Spark jobs. Run from the repo root:
//   scala-cli compile legacy
//   scala-cli run legacy --main-class lucid.telemetry.GenerateExpected
//> using scala 2.13.14
//> using dep org.apache.spark::spark-sql:3.5.3
//> using dep org.apache.spark::spark-sql-kafka-0-10:3.5.3
// Spark 3.5 on Java 17 needs these module opens (spark-submit normally adds them).
//> using javaOpt --add-opens=java.base/java.lang=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.util=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
