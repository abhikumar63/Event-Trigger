package com.example.alerting;

import com.example.alerting.models.AlertTriggered;
import com.example.alerting.models.ItemRecord;
import com.example.alerting.models.Rule;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AlertingJob {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(AlertingJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String brokers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (brokers == null || brokers.isEmpty()) {
            brokers = "kafka:29092";
        }

        // Output Tag for DLQ
        final OutputTag<String> dlqTag = new OutputTag<String>("dlq-orders-topic"){};

        // 1. Rule Source
        KafkaSource<String> ruleSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("rule-stream-topic")
                .setGroupId("flink-rule-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<Rule> ruleStream = env
                .fromSource(ruleSource, WatermarkStrategy.noWatermarks(), "Rule Source")
                .map(json -> mapper.readValue(json, Rule.class))
                .returns(Rule.class);

        // 2. Order Source (Raw Bytes)
        KafkaSource<byte[]> orderSource = KafkaSource.<byte[]>builder()
                .setBootstrapServers(brokers)
                .setTopics("raw-orders-topic")
                .setGroupId("flink-order-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<byte[]>() {
                    @Override
                    public byte[] deserialize(byte[] message) throws IOException {
                        return message;
                    }
                })
                .build();

        DataStream<byte[]> rawOrderStream = env
                .fromSource(orderSource, WatermarkStrategy.noWatermarks(), "Order Source");

        // 3. Broadcast State Descriptor
        MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(Rule.class)
        );

        BroadcastStream<Rule> broadcastRules = ruleStream.broadcast(ruleStateDescriptor);

        // 4. Deserialization & Fan-out with DLQ (ProcessFunction)
        SingleOutputStreamOperator<ItemRecord> itemStream = rawOrderStream
                .process(new ProcessFunction<byte[], ItemRecord>() {
                    private transient KafkaAvroDeserializer deserializer;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        deserializer = new KafkaAvroDeserializer();
                        Map<String, Object> config = new HashMap<>();
                        
                        String schemaRegistryUrl = System.getenv("SCHEMA_REGISTRY_URL");
                        if (schemaRegistryUrl == null || schemaRegistryUrl.isEmpty()) {
                            schemaRegistryUrl = "http://schema-registry:8081";
                        }
                        
                        config.put("schema.registry.url", schemaRegistryUrl);
                        deserializer.configure(config, false);
                    }

                    @Override
                    public void processElement(byte[] value, Context ctx, Collector<ItemRecord> out) throws Exception {
                        try {
                            GenericRecord record = (GenericRecord) deserializer.deserialize("raw-orders-topic", value);
                            
                            Long timestamp = (Long) record.get("timestamp");
                            Object itemsObj = record.get("items");
                            
                            if (itemsObj instanceof Iterable) {
                                for (Object itemObj : (Iterable<?>) itemsObj) {
                                    GenericRecord item = (GenericRecord) itemObj;
                                    String productId = item.get("productId").toString();
                                    int quantity = (Integer) item.get("quantity");
                                    out.collect(new ItemRecord(productId, timestamp, quantity));
                                }
                            }
                        } catch (Exception e) {
                            String base64Garbage = Base64.getEncoder().encodeToString(value);
                            LOG.warn("[DLQ CAUGHT POISON PILL] Failed to deserialize Avro payload: {}", e.getMessage());
                            ctx.output(dlqTag, base64Garbage);
                        }
                    }
                })
                .returns(ItemRecord.class);

        // 5. DLQ Sink
        DataStream<String> dlqStream = itemStream.getSideOutput(dlqTag);
        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("dlq-orders-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
        dlqStream.sinkTo(dlqSink);

        // 6. Main Flow: Connect Streams and Process Alerts
        DataStream<AlertTriggered> alerts = itemStream
                .keyBy(item -> item.productId)
                .connect(broadcastRules)
                .process(new DynamicAlertFunction(ruleStateDescriptor));

        // 7. Alert Sink
        KafkaSink<String> alertSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("triggered-alerts-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        alerts
                .map(alert -> mapper.writeValueAsString(alert))
                .sinkTo(alertSink);

        env.execute("Dynamic Alerting Engine");
    }

    public static class DynamicAlertFunction extends KeyedBroadcastProcessFunction<String, ItemRecord, Rule, AlertTriggered> {

        private final MapStateDescriptor<String, Rule> ruleStateDescriptor;

        private transient ListState<ItemRecord> windowItems;
        private transient ValueState<Long> lastEmittedTime;

        public DynamicAlertFunction(MapStateDescriptor<String, Rule> ruleStateDescriptor) {
            this.ruleStateDescriptor = ruleStateDescriptor;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.hours(12))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ListStateDescriptor<ItemRecord> tsDescriptor = new ListStateDescriptor<>("windowItems", ItemRecord.class);
            tsDescriptor.enableTimeToLive(ttlConfig);
            windowItems = getRuntimeContext().getListState(tsDescriptor);

            ValueStateDescriptor<Long> emittedTimeDescriptor = new ValueStateDescriptor<>("lastEmittedTime", Long.class);
            emittedTimeDescriptor.enableTimeToLive(ttlConfig);
            lastEmittedTime = getRuntimeContext().getState(emittedTimeDescriptor);
        }

        @Override
        public void processElement(ItemRecord itemRecord, ReadOnlyContext ctx, Collector<AlertTriggered> out) throws Exception {
            Rule rule = ctx.getBroadcastState(ruleStateDescriptor).get(itemRecord.productId);
            if (rule == null) {
                return; 
            }

            long eventTime = itemRecord.timestamp;
            windowItems.add(itemRecord);

            long windowStart = eventTime - (rule.windowSeconds * 1000L);
            Iterator<ItemRecord> iterator = windowItems.get().iterator();
            List<ItemRecord> retainedItems = new ArrayList<>();
            int currentQuantitySum = 0;

            while (iterator.hasNext()) {
                ItemRecord rec = iterator.next();
                if (rec.timestamp >= windowStart) {
                    retainedItems.add(rec);
                    currentQuantitySum += rec.quantity;
                }
            }

            windowItems.update(retainedItems);

            if (currentQuantitySum >= rule.threshold) {
                Long lastEmitted = lastEmittedTime.value();
                long requiredCooldownMs = (rule.ttlSeconds * 1000L) / 2;

                if (lastEmitted == null || (eventTime - lastEmitted) >= requiredCooldownMs) {
                    out.collect(new AlertTriggered(itemRecord.productId, rule.ttlSeconds, currentQuantitySum));
                    lastEmittedTime.update(eventTime);
                }
            }
        }

        @Override
        public void processBroadcastElement(Rule rule, Context ctx, Collector<AlertTriggered> out) throws Exception {
            ctx.getBroadcastState(ruleStateDescriptor).put(rule.productID, rule);
        }
    }
}
