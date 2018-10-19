package com.bettercloud.kafka.kreader;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.server.ServerResponse;
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
    public Mono<ResponseEntity<Object>> records(@PathVariable String topic,
                                        @PathVariable int partition,
                                        @RequestParam(defaultValue = "100") int size,
                                        @RequestParam(defaultValue = "0") long offset) {
        if(size > 10000)
            return Mono.just(ResponseEntity.badRequest().body("Query param 'size' must be less than " + 10000));

        final Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        final ReceiverOptions<String, Object> receiverOptions = ReceiverOptions.<String, Object>create(consumerProps)
                .consumerProperty(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
                .addAssignListener(assignedPartitions -> assignedPartitions.forEach(p -> p.seek(offset)))
                .assignment(Collections.singletonList(new TopicPartition(topic, partition)));
        return KafkaReceiver.create(receiverOptions)
                .receive()
                .take(size)
                .take(READ_TIMEOUT)
                .collectList()
                .map(records -> toRecordPage(topic, partition, offset, records))
                .map(page -> ResponseEntity.ok().body(page));
    }

    private RecordPage toRecordPage(String topic, int partition, long incomingOffset, List<ReceiverRecord<String, Object>> records) {
        List<Record> r = records.stream()
                .map(this::toJson)
                .collect(Collectors.toList());

        final long nextOffset;
        if (!records.isEmpty()) {
            nextOffset = records.get(r.size() - 1).offset() + 1;
        } else {
            nextOffset = incomingOffset;
        }

        return new RecordPage(topic, partition, incomingOffset, nextOffset, r);
    }

    private Record toJson(ReceiverRecord<String, Object> record) {
        try {
            return new Record(record.key(), record.offset(), objectMapper.readTree(record.value().toString()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
