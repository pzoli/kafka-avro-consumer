package hu.infokristaly.kafkaconsumer;

import java.util.HashMap;
import java.util.Map;

import hu.infokristaly.kafkaconsumer.avro.User;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.annotation.TopicPartition;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.properties.sasl.jaas.config}")
    private String kafka_jaas_config;

    @Value("${avro.schema_registry.url}")
    private String avro_schema_registry_url ;

    @Value("${spring.kafka.properties.sasl.mechanism}")
    private String sasl_mechanism;

    @Value("${spring.kafka.properties.security.protocol}")
    private String security_protocol;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapAddress);
        configProps.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId);
        configProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        configProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        configProps.put(
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.RangeAssignor");
        configProps.put(
                SaslConfigs.SASL_MECHANISM, sasl_mechanism);
        configProps.put(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                security_protocol);
        configProps.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                kafka_jaas_config);
        configProps.put(
                "schema.registry.url", avro_schema_registry_url);

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    
    //@KafkaListener(id = "avro-test", groupId = "${spring.kafka.consumer.group-id}", topicPartitions= {
    //        @TopicPartition(topic = "avro-test", partitions = { "0" })})
    @KafkaListener(id = "avro-test", groupId = "${spring.kafka.consumer.group-id}", topics = "avro-test")
    public void listenGroupFoo(ConsumerRecord<String, User> user) {
        System.out.println("Received Message in group1: " + user.value());
    }
}