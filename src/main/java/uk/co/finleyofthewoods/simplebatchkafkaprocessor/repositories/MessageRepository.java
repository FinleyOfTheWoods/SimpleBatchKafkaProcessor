package uk.co.finleyofthewoods.simplebatchkafkaprocessor.repositories;


import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import uk.co.finleyofthewoods.simplebatchkafkaprocessor.models.Message;

@Repository
public interface MessageRepository extends JpaRepository<Message, Long> {}
