package uk.co.finleyofthewoods.simplebatchkafkaprocessor.configuration;

import jakarta.persistence.EntityManagerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.database.JpaItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JpaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.task.ThreadPoolTaskExecutorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import uk.co.finleyofthewoods.simplebatchkafkaprocessor.models.Message;
import uk.co.finleyofthewoods.simplebatchkafkaprocessor.processors.MessageItemProcessor;
import uk.co.finleyofthewoods.simplebatchkafkaprocessor.readers.KafkaItemReader;

import java.util.Properties;

@Configuration
public class BatchConfiguration {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.topic}")
    private String topic;

    @Value("${spring.kafka.consumer.session-timeout-ms:30000}")
    private String sessionTimeout;

    @Value("${spring.kafka.consumer.heartbeat-interval-ms:10000}")
    private String heartbeatInterval;

    @Value("${spring.kafka.consumer.max-poll-interval-ms:300000}")
    private String maxPollInterval;

    @Value("${spring.kafka.consumer.poll-timeout:1000}")
    private long pollTimeout;

    @Value("${spring.batch.job.name")
    private String jobName;

    @Value("${spring.batch.step.name}")
    private String stepName;

    @Value("${spring.batch.job.chunk-size:10}")
    private int chunkSize;

    @Bean
    public KafkaItemReader kafkaItemReader() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);

        return new KafkaItemReader(properties, topic, pollTimeout);
    }

    @Bean
    public JpaItemWriter<Message> jpaItemWriter(EntityManagerFactory entityManagerFactory) {
        return new JpaItemWriterBuilder<Message>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }

    @Bean
    public AsyncTaskExecutor taskExecutor() {
        return new ThreadPoolTaskExecutorBuilder()
                .corePoolSize(4)
                .maxPoolSize(10)
                .queueCapacity(25)
                .build();
    }

    @Bean
    public Step step(JobRepository jobRepository, AsyncTaskExecutor taskExecutor, KafkaItemReader kafkaItemReader,
                     MessageItemProcessor messageItemProcessor, JpaItemWriter<Message> jpaItemWriter) {
        return new StepBuilder(stepName, jobRepository)
                .<Message, Message>chunk(chunkSize)
                .reader(kafkaItemReader)
                .processor(messageItemProcessor)
                .writer(jpaItemWriter)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public Job job(JobRepository jobRepository, Step step) {
        return new JobBuilder(jobName, jobRepository).start(step).build();
    }
}
