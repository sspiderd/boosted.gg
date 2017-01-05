def ISO8601 = timestamp("yyyy-MM-dd'T'HH:mm:ss")

appender("STDOUT", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "${ISO8601} %-5level [%thread] %logger{15} - %msg%n"
    }
}

logger("org.apache.spark.mllib.clustering", ERROR)
logger("gg.boosted", DEBUG)

root(WARN, ["STDOUT"])