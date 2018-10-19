package com.bettercloud.kafka.kreader;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RestController
public class KafkaController {

    @GetMapping("/topics/{topic}/partitions/{partition}")
    @ResponseBody
    public Mono<RecordPage> records(@PathVariable String topic,
                                    @PathVariable int partition,
                                    @RequestParam(defaultValue = "100") int maxRecords) {
        final Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaDe.class);
        final ReceiverOptions<String, Object> recieverOptions = ReceiverOptions.<String, Object>create()
                .assignment(Collections.singletonList(new TopicPartition(topic, partition)));

        return KafkaReceiver.create(recieverOptions)
                .receive()
                .take(maxRecords)
                .collectList()
                .map(records -> new RecordPage(topic, partition, 321L, records));
    }
}
