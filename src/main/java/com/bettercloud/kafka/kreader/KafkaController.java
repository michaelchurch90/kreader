package com.bettercloud.kafka.kreader;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RestController
public class KafkaController {

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
                .consumerProperty("schema.registry.url", schemaRegistryUrl)
                .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

                .assignment(Collections.singletonList(new TopicPartition(topic, partition)));
        return KafkaReceiver.create(recieverOptions)
                .receive()
                .doOnNext(System.out::println)
                .take(2)
                .map(ConsumerRecord::value)
                .map(Object::toString)
                .take(1)
//                .then(Mono.just(new RecordPage(topic, partition, 123, Collections.emptyList())));
                .collectList()
                .map(records -> new RecordPage(topic, partition, 321L, records));
//                .doOnError(e -> System.err.println(e.toString()));
    }
}
