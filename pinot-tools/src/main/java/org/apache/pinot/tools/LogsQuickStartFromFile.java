/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LogsQuickStartFromFile extends Quickstart {
  public static final String TABLE_NAME = "logs";
  public static final String WRITE_PATH = "examples/stream/" + TABLE_NAME;
  public static final String QUICK_START_NAME = "LOGS_FROM_FILE";
  public static final String TOPIC_NAME = TABLE_NAME;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(LogsQuickStartFromFile.class);

  private final String _logFilePath = "/Users/spyne/Downloads/archive/access.log";
  private final AtomicLong _messageCounter = new AtomicLong(0);

  public LogsQuickStartFromFile() {
  }

  public static void main(String... args) throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", QUICK_START_NAME));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  private static KafkaProducer<String, String> createKafkaProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:19092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return new KafkaProducer<>(props);
  }

  @Override
  public String[] getDefaultBatchTableDirectories() {
    return new String[0];
  }

  protected Map<String, String> getDefaultStreamTableDirectories() {
    return Map.of(TOPIC_NAME, WRITE_PATH);
  }

  @Override
  public List<String> types() {
    return Collections.singletonList(QUICK_START_NAME);
  }

  @Override
  protected Map<String, Object> getConfigOverrides() {
    return Map.of();
  }

  @Override
  public void execute() throws Exception {
    File quickstartTmpDir = _setCustomDataDir ? _dataDir : new File(_dataDir,
        String.valueOf(System.currentTimeMillis()));
    File quickstartRunnerDir = new File(quickstartTmpDir, "quickstart");
    Preconditions.checkState(quickstartRunnerDir.mkdirs());
    List<QuickstartTableRequest> quickstartTableRequests = bootstrapStreamTableDirectories(quickstartTmpDir);

    System.out.println("***** Starting realtime quick start *****");
    startKafka();
    final QuickstartRunner runner = new QuickstartRunner(quickstartTableRequests,
        1, 1, 1, 1,
        quickstartRunnerDir,
        getConfigOverrides());

    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, broker, server and minion *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down realtime quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        LOG.error("Caught exception while shutting down", e);
      }
    }));

    printStatus(Color.CYAN, "***** Bootstrap all tables *****");
    runner.bootstrapTable();

    printStatus(Color.YELLOW, "***** Realtime quickstart setup complete *****");
    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
    printStatus(Color.CYAN, "***** Reading and pushing messages from log file to Kafka *****");
    pushMessages();
  }

  private void pushMessages() throws IOException {
    try (KafkaProducer<String, String> producer = createKafkaProducer();
        BufferedReader reader = new BufferedReader(new FileReader(_logFilePath))) {

      String line;
      while ((line = reader.readLine()) != null) {
        producer.send(newMessage(line));
      }

      // Keep the program running to allow querying
      printStatus(Color.GREEN, "***** Finished pushing all log entries. Press Ctrl+C to exit *****");
      while (true) {
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      LOG.error("Interrupted while sleeping", e);
    }
  }

  private ProducerRecord<String, String> newMessage(String logLine) throws JsonProcessingException {
    LogMessage payload = new LogMessage()
        .setTs(String.valueOf(parseLogTimestamp(logLine)))
        .setlogLine(logLine);
    return toKafkaRecord(payload);
  }

  public long parseLogTimestamp(String logLine) {
    // Extract timestamp portion between brackets
    int startIndex = logLine.indexOf('[') + 1;
    int endIndex = logLine.indexOf(']');
    String timestamp = logLine.substring(startIndex, endIndex);

    // Create formatter matching the log format "dd/MMM/yyyy:HH:mm:ss +ZZZZ"
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

    // Parse the timestamp to LocalDateTime
    LocalDateTime dateTime = LocalDateTime.parse(timestamp, formatter);

    // Convert to epoch seconds
    return dateTime.toEpochSecond(ZoneOffset.of("+0330"));
  }

  private ProducerRecord<String, String> toKafkaRecord(LogMessage payload) throws JsonProcessingException {
    // Use an incrementing counter as the key
    String key = String.valueOf(_messageCounter.incrementAndGet());
    return new ProducerRecord<>(TOPIC_NAME, key, OBJECT_MAPPER.writeValueAsString(payload));
  }

  // CHECKSTYLE:OFF
  private static class LogMessage {
    private String ts;
    private String logLine;

    public String getTs() {
      return ts;
    }

    public LogMessage setTs(String ts) {
      this.ts = ts;
      return this;
    }

    public String getlogLine() {
      return logLine;
    }

    public LogMessage setlogLine(String logLine) {
      this.logLine = logLine;
      return this;
    }
  }
  // CHECKSTYLE:ON
}
