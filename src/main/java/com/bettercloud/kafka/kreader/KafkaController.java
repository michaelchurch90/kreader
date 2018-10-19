package com.bettercloud.kafka.kreader;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@RestController
public class KafkaController {
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);
    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private static final String AVRO_DESERIALIZAER = "Avro";
    private static final String STRING_DESERIALIZAER = "String";
    private static final String LONG_MAX_VALUE = "9223372036854775807";
    private static final int MAX_SIZE = 10000;
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
                                                @RequestParam(defaultValue = AVRO_DESERIALIZAER) String serialization,
                                                @RequestParam(defaultValue = "") String filter,
                                                @RequestParam(defaultValue = LONG_MAX_VALUE) Long before,
                                                @RequestParam(defaultValue = "0") Long after,
                                                @RequestParam(defaultValue = "10") int size,
                                                @RequestParam(defaultValue = "0") long offset) {
        if (size > MAX_SIZE)
            return Mono.just(ResponseEntity.badRequest().body("Query param 'size' must be less than " + 10000));

        Object deserializer = KafkaAvroDeserializer.class;
        Pattern pattern = Pattern.compile(filter, Pattern.DOTALL);

        if (serialization.equals(STRING_DESERIALIZAER))
            deserializer = StringDeserializer.class;

        final Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);


        final ReceiverOptions<String, Object> receiverOptions = ReceiverOptions.<String, Object>create(consumerProps)
                .consumerProperty(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
                .addAssignListener(assignedPartitions -> assignedPartitions.forEach(p -> p.seek(offset)))
                .assignment(Collections.singletonList(new TopicPartition(topic, partition)));

        return KafkaReceiver.create(receiverOptions)
                .receive()
                .filter(record -> pattern.matcher(record.toString()).find())
                .filter(record -> record.timestamp() < before && record.timestamp() > after)
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
