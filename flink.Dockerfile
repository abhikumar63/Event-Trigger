# Use the exact same base image you were already using
FROM flink:1.17-java11

# Flink 1.17 uses Log4j 2.17.1. 
# We use wget to download the exact matching JSON template JAR directly into Flink's lib folder.
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-layout-template-json/2.17.1/log4j-layout-template-json-2.17.1.jar