package com.bettercloud.kafka.kreader;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class KafkaController {

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);
    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String bootstrapServers;
    private final String schemaRegistryUrl;


    public KafkaController(@Value("${bootstrap.servers}") String bootstrapServers,
                           @Value("${schema.registry.url}") String schemaRegistryUrl) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @GetMapping("/topics/{topic}/partitions/{partition}")
    @ResponseBody
    public Mono<RecordPage> records(@PathVariable String topic,
                                    @PathVariable int partition,
                                    @RequestParam(defaultValue = "100") int maxRecords) {
        final Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        final ReceiverOptions<String, Object> recieverOptions = ReceiverOptions.<String, Object>create(consumerProps)
                .consumerProperty(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
                .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .assignment(Collections.singletonList(new TopicPartition(topic, partition)));
        return KafkaReceiver.create(recieverOptions)
                .receive()
                .doOnNext(v -> System.out.println(v.value()))
                .take(maxRecords)
                .take(READ_TIMEOUT)
                .collectList()
                .map(records -> toRecordPage(topic, partition, records))
                .doOnError(e -> System.err.println(e.toString()));
    }

    private RecordPage toRecordPage(@PathVariable String topic, @PathVariable int partition, List<ReceiverRecord<String, Object>> records) {

        List<Record> r = records.stream()
                .map(this::toJson)
                .collect(Collectors.toList());

        return new RecordPage(topic, partition, 321L, r);
    }

    private Record toJson(ReceiverRecord<String, Object> record) {
        try {
            return new Record(record.topic(), record.partition(), record.offset(), objectMapper.readTree(record.value().toString()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
