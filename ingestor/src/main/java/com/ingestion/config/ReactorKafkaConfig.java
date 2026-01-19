package com.ingestion.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ReactorKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public KafkaSender<String, String> reactorKafkaSender() {
        Map<String, Object> props = new HashMap<>();

        // 기본 연결 설정
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 배치 및 성능 설정
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);           // 배치 대기시간
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);       // 32KB 배치
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"); // LZ4 압축
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64MB 버퍼

        // 신뢰성 설정
        props.put(ProducerConfig.ACKS_CONFIG, "1");               // 리더 확인
        props.put(ProducerConfig.RETRIES_CONFIG, 3);              // 재시도 횟수
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // 타임아웃 설정
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);    // 요청 타임아웃 5초
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);  // 전송 타임아웃 10초
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);          // 메타데이터 대기 5초

        // 연결 관리
        props.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60000);
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 5000);

        SenderOptions<String, String> senderOptions = SenderOptions.<String, String>create(props)
            .stopOnError(false)   // 에러 발생해도 계속 진행
            .maxInFlight(1024);   // 동시 전송 요청 수

        return KafkaSender.create(senderOptions);
    }
}
