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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.glassfish.tyrus.client.ClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetailOrderLineStream {

  public static final String KAFKA_TOPIC = "retailOrderLines";
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected static final Logger LOGGER = LoggerFactory.getLogger(RetailOrderLineStream.class);
  private static int NEXT_ORDER_NO = 0;
  private static Map<Status, Status> TRANSITION_MAP = ImmutableMap.of(
      Status.ORDERED, Status.PROCESSING,
      Status.PROCESSING, Status.SHIPPED,
      Status.SHIPPED, Status.DELIVERED
  );

  protected final boolean _partitionByKey;
  protected final StreamDataProducer _producer;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Random random = new Random();
  protected ClientManager _client;
  protected volatile boolean _keepPublishing;
  private Queue<OrderLine> q = new ArrayDeque<>();
  private long counter = 0;

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

    scheduledExecutorService.scheduleAtFixedRate(this::handleMessage, 10, 1, TimeUnit.SECONDS);
  }

  public void stopPublishing() {
    scheduledExecutorService.shutdown();
    _keepPublishing = false;
    _client.shutdown();
    _producer.close();
  }

  private void handleMessage() {
    try {
      for (OrderLine ol : next()) {
        pushToKafka(ol);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing the message", e);
    }
  }

  private void pushToKafka(final Object o) throws JsonProcessingException {
    final String payloadString = OBJECT_MAPPER.writeValueAsString(o);
    if (_keepPublishing) {
      if (_partitionByKey) {
        _producer.produce(KAFKA_TOPIC, StringUtil.encodeUtf8(String.valueOf(counter)),
            StringUtil.encodeUtf8(payloadString));
      } else {
        _producer.produce(KAFKA_TOPIC, StringUtil.encodeUtf8(payloadString));
      }
    }
  }

  private List<OrderLine> next() {
    List<OrderLine> orderLines = new ArrayList<>();
    if (flip(0.8)) {
      List<OrderLine> processed = new ArrayList<>();
      while (!q.isEmpty()) {
        OrderLine ol = q.poll();
        if (flip(0.3)) {
          final OrderLine transitioned = transition(ol);
          if (transitioned != null) {
            orderLines.add(transitioned);
            processed.add(transitioned);
          }
        } else {
          processed.add(ol);
        }
      }
      q.addAll(processed);
      return orderLines;
    } else {
      final int nextOrderNo = ++NEXT_ORDER_NO;
      orderLines.add(newOrderLine(nextOrderNo, 1));
      if (flip(0.5)) {
        orderLines.add(newOrderLine(nextOrderNo, 2));
        if (flip(0.5)) {
          orderLines.add(newOrderLine(nextOrderNo, 3));
          if (flip(0.5)) {
            orderLines.add(newOrderLine(nextOrderNo, 4));
          }
        }
      }
      q.addAll(orderLines);
      return orderLines;
    }
  }

  private OrderLine transition(final OrderLine ol) {
    final Status status = TRANSITION_MAP.get(ol.getStatus());
    if (status == null) {
      return null;
    }
    return ol
        .setEpochMillis(System.currentTimeMillis())
        .setStatus(status);
  }

  private boolean flip(final double v) {
    return random.nextFloat() <= v;
  }

  private OrderLine newOrderLine(final int orderNo, final int orderLineNo) {
    return new OrderLine()
        .setEpochMillis(System.currentTimeMillis())
        .setOrderNo(String.valueOf(orderNo))
        .setOrderLineNo(String.valueOf(orderLineNo))
        .setItemID(String.valueOf(orderLineNo))
        .setOrderedQuantity(1)
        .setStatus(Status.ORDERED)
        .setTotal(100.0);
  }

  public enum Status {ORDERED, PROCESSING, SHIPPED, DELIVERED}

  public static class OrderLine {

    long epochMillis;
    String orderNo;
    String orderLineNo;
    String itemID;
    Status status;
    Double total;
    Integer orderedQuantity;

    public long getEpochMillis() {
      return epochMillis;
    }

    public OrderLine setEpochMillis(final long epochMillis) {
      this.epochMillis = epochMillis;
      return this;
    }

    public String getOrderNo() {
      return orderNo;
    }

    public OrderLine setOrderNo(final String orderNo) {
      this.orderNo = orderNo;
      return this;
    }

    public String getOrderLineNo() {
      return orderLineNo;
    }

    public OrderLine setOrderLineNo(final String orderLineNo) {
      this.orderLineNo = orderLineNo;
      return this;
    }

    public String getItemID() {
      return itemID;
    }

    public OrderLine setItemID(final String itemID) {
      this.itemID = itemID;
      return this;
    }

    public Status getStatus() {
      return status;
    }

    public OrderLine setStatus(final Status status) {
      this.status = status;
      return this;
    }

    public Double getTotal() {
      return total;
    }

    public OrderLine setTotal(final Double total) {
      this.total = total;
      return this;
    }

    public Integer getOrderedQuantity() {
      return orderedQuantity;
    }

    public OrderLine setOrderedQuantity(final Integer orderedQuantity) {
      this.orderedQuantity = orderedQuantity;
      return this;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final OrderLine orderLine = (OrderLine) o;
      return epochMillis == orderLine.epochMillis && Objects.equals(orderNo, orderLine.orderNo)
          && Objects.equals(orderLineNo, orderLine.orderLineNo);
    }

    @Override
    public int hashCode() {
      return Objects.hash(epochMillis, orderNo, orderLineNo);
    }
  }
}
