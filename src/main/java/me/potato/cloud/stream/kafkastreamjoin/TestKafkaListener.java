package me.potato.cloud.stream.kafkastreamjoin;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TestKafkaListener {
    @KafkaListener(groupId = "temp", topics = "output-topic")
    public void on(Object message){
      log.info("test output {}", message.toString());
    }
}
