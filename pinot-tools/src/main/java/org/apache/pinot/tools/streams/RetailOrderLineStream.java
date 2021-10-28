/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.streams;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.glassfish.tyrus.client.ClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RetailOrderLineStream {

  protected static final Logger LOGGER = LoggerFactory.getLogger(RetailOrderLineStream.class);
  public static final String KAFKA_TOPIC = "mockEvent";

  protected final boolean _partitionByKey;
  protected final StreamDataProducer _producer;

  protected ClientManager _client;
  protected volatile boolean _keepPublishing;
  private final ScheduledExecutorService scheduledExecutorService;

  private long counter = 0;
  private long backFillCounter = 0;
  private final long backfillTime = System.currentTimeMillis();
  private final Random random = new Random();

  public RetailOrderLineStream()
      throws Exception {
    this(false);
  }

  public RetailOrderLineStream(boolean partitionByKey)
      throws Exception {
    _partitionByKey = partitionByKey;

    Properties properties = new Properties();
    properties.put("metadata.broker.list", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    _producer = StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME,
        properties);
    scheduledExecutorService = Executors.newScheduledThreadPool(1);
  }

  public void run()
      throws Exception {
    _client = ClientManager.createClient();
    _keepPublishing = true;

    scheduledExecutorService.scheduleAtFixedRate(this::handleMessage,10, 59, TimeUnit.SECONDS);
//    scheduledExecutorService.schedule(this::backfill, 20, TimeUnit.SECONDS);
  }

  private void backfill() {
    System.out.println("STARTING BACKFILL!!!");
    System.out.println("======================================");
    try {
      for (int i = 0; i < 10000; i++) {
        final int spike = random.nextDouble() <= 0.1 ? 1000 : 0;
        pushToKafka(newNode(backfillTime - (i * 60_000L),
            200 + (100 * Math.sin(++backFillCounter)) + spike));
        Thread.sleep(0);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing the message", e);
    }
  }

  public void stopPublishing() {
    scheduledExecutorService.shutdown();
    _keepPublishing = false;
    _client.shutdown();
    _producer.close();
  }

  private void handleMessage() {
    try {
      final int spike = random.nextDouble() <= 0.2 ? 2000 : 0;
      final double value = 200 + (100 * Math.sin(++counter)) + spike;

      final long timeMillis = System.currentTimeMillis();
      pushToKafka(newNode(timeMillis, value));
      LOGGER.info(String.format("time: %d, val: %f", timeMillis, value));
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing the message", e);
    }
  }

  private void pushToKafka(final ObjectNode json) {
    final String payloadString = json.toString();
    if (_keepPublishing) {
      if (_partitionByKey) {
        _producer.produce(KAFKA_TOPIC, StringUtil.encodeUtf8(String.valueOf(counter)),
            StringUtil.encodeUtf8(payloadString));
      } else {
        _producer.produce(KAFKA_TOPIC, StringUtil.encodeUtf8(payloadString));
      }
    }
  }

  private ObjectNode newNode(final long timeMillis, final double value) {
    ObjectNode json = JsonUtils.newObjectNode();
    json.put("m1", value);
    json.put("d1", "DEFAULT");
    json.put("mtime", timeMillis);
    return json;
  }
}
