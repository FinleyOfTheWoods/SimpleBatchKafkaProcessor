package uk.co.finleyofthewoods.simplebatchkafkaprocessor.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;
import uk.co.finleyofthewoods.simplebatchkafkaprocessor.models.Message;

@Component
public class MessageItemProcessor implements ItemProcessor<Message, Message> {
    private static final Logger logger = LoggerFactory.getLogger(MessageItemProcessor.class);

    @Override
    public Message process(Message message) throws Exception {
        logger.debug("Processing message: {}", message);
        return message;
    }
}
