package me.potato.cloud.stream.kafkastreamjoin;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@SpringBootApplication
public class KafkaStreamJoinApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamJoinApplication.class, args);
    }

    @EnableBinding(SimpleStreamProcessor.class)
    public static class KStreamToTableJoinApplication {
        @StreamListener
        @SendTo("output")
        public KStream<String, Long> process(@Input("input1") KStream<String, Long> userClicksStream,
                                             @Input("input2") KTable<String, String> userRegionsTable) {

            return userClicksStream
                    .leftJoin(userRegionsTable
                            , (clicks, region) -> new RegionWithClicks(region == null ? "UNKNOWN" : region, clicks)
                            , Joined.with(Serdes.String(), Serdes.Long(), null))
                    .map((region, regionWithClicks) -> new KeyValue<>(regionWithClicks.getRegion(), regionWithClicks.getClicks()))
                    .groupByKey(Serialized.with(Serdes.String(), Serdes.Long()))
                    .reduce((firstClicks, secondClicks) -> firstClicks + secondClicks, Materialized.as("sampleTable"))
                    .toStream();
        }
    }

    interface SimpleStreamProcessor extends KafkaStreamsProcessor {
        // kstream은 insert
        @Input("input1")
        KStream clicks();

        // Ktable은 upsert(위치가 변경되는데로 업데이트 된다고 보면 됨)
        @Input("input2")
        KTable region();
    }

    @Slf4j
    @Getter
    @ToString
    private static final class RegionWithClicks {

        private final String region;
        private final long clicks;

        public RegionWithClicks(String region, long clicks) {
            if (region == null || region.isEmpty()) {
                throw new IllegalArgumentException("region must be set");
            }
            if (clicks < 0) {
                throw new IllegalArgumentException("clicks must not be negative");
            }
            this.region = region;
            this.clicks = clicks;

            log.info(this.toString());
        }

    }

    @RestController
    public static class CountRestController {

        private final InteractiveQueryService service;

        public CountRestController(InteractiveQueryService service) {
            this.service = service;
        }

        @GetMapping("/counts")
        Map<String, Long> counts() throws InterruptedException {

            Map<String, Long> counts = new HashMap<>();
            ReadOnlyKeyValueStore<String, Long> output = service.getQueryableStore("sampleTable", QueryableStoreTypes.keyValueStore());

            KeyValueIterator<String, Long> all = output.all();
            while (all.hasNext()) {
                KeyValue<String, Long> next = all.next();
                counts.put(next.key, next.value);
            }

            return counts;

        }

    }
}
