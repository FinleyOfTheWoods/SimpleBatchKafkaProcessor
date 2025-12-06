package uk.co.finleyofthewoods.simplebatchkafkaprocessor.readers;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.NonTransientResourceException;
import org.springframework.batch.infrastructure.item.support.AbstractItemStreamItemReader;
import uk.co.finleyofthewoods.simplebatchkafkaprocessor.models.Message;

import java.time.Duration;
import java.util.*;

public class KafkaItemReader extends AbstractItemStreamItemReader<Message> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaItemReader.class);

    private final Properties properties;
    private final String topic;
    private final long pollTimeout;

    private KafkaConsumer<String, String> consumer;
    private Iterator<ConsumerRecord<String, String>> iterator;
    private List<ConsumerRecord<String, String>> batch;

    public KafkaItemReader(Properties properties, String topic, long pollTimeout) {
        this.properties = properties;
        this.topic = topic;
        this.pollTimeout = pollTimeout;

        batch = new ArrayList<>();
        setName("kafkaItemReader");
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);

        try {
            this.consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singletonList(topic));
            logger.info("{} opened and subscribed to topic {}", KafkaItemReader.class.getSimpleName(), topic);
        } catch (Exception e) {
            logger.error("Failed to open {}", KafkaItemReader.class.getSimpleName(), e);
        }
    }

    @Override
    public Message read() throws Exception {
        try {
            if (iterator == null || !iterator.hasNext()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));

                if (records.isEmpty()) {
                    logger.debug("No records found in Kafka topic {}.", topic);
                    return null;
                }

                batch.clear();
                records.forEach(batch::add);
                iterator = batch.iterator();
                logger.debug("Polled {} records from Kafka topic {}.", records.count(), topic);
            }
            if (iterator.hasNext()) {
                ConsumerRecord<String, String> record = iterator.next();
                logger.trace("Reading message - Topic: {}, Partition: {}, Offset: {}",
                        record.topic(), record.partition(), record.offset());
                return new Message(record.key(), record.value(), record.topic(), record.partition(), record.offset());
            }
            return null;
        } catch (WakeupException e) {
            logger.info("Kafka consumer wakeup called");
            return null;
        } catch (Exception e) {
            logger.error("Failed to read from Kafka topic {}", topic, e);
            throw new NonTransientResourceException("Failed to read from Kafka topic: " + topic, e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) {
        super.update(executionContext);

        if (consumer != null ) {
            try {
                consumer.commitSync();
                logger.debug("Committed offsets to Kafka topic {}", topic);
            } catch (Exception e) {
                logger.error("Failed to commit offsets to Kafka topic {}", topic, e);
                throw new ItemStreamException("Failed to commit offsets to Kafka topic: " + topic, e);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        if (consumer != null) {
            consumer.close();
        }
    }

    @PreDestroy
    public void shutdown() {
        closeConsumer();
    }

    private void closeConsumer() {
        if (consumer != null) {
            try {
                consumer.wakeup();
                consumer.close();
                logger.info("{} closed successfully", KafkaItemReader.class.getSimpleName());
            } catch (Exception e) {
                logger.error("Failed to close {}", KafkaItemReader.class.getSimpleName(), e);
            } finally {
                consumer = null;
                iterator = null;
                batch.clear();
            }
        }
    }
}


//public class KafkaItemReader implements ItemReader<Message> {
//    private static final Logger logger = LoggerFactory.getLogger(KafkaItemReader.class);
//
//    private final KafkaConsumer<String, String> consumer;
//    private Iterator<ConsumerRecord<String, String>> iterator;
//    private final long pollTimeout;
//    private final String topic;
//    private List<ConsumerRecord<String, String>> batch = new ArrayList<>();
//
//    public KafkaItemReader(Properties properties, String topic, long pollTimeout) {
//        this.consumer = new KafkaConsumer<>(properties);
//        this.pollTimeout = pollTimeout;
//        this.topic = topic;
//
//        consumer.subscribe(Collections.singletonList(topic));
//    }
//
//    @Override
//    public Message read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
//        if (iterator == null || !iterator.hasNext()) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));
//            if (records.isEmpty()) return null;
//
//            batch.clear();
//            records.forEach(batch::add);
//            iterator = batch.iterator();
//        }
//        if (iterator.hasNext()) {
//            ConsumerRecord<String, String> record = iterator.next();
//            return new Message(record.key(), record.value(), record.topic(), record.partition(), record.offset());
//
//        }
//        return null;
//    }
//
//
//    @PreDestroy
//    public void shutdown() {
//        close();
//    }
//
//    private void close() {
//        if (consumer != null) consumer.close();
//    }
//}
