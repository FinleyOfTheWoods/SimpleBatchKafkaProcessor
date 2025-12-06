package uk.co.finleyofthewoods.simplebatchkafkaprocessor.models;

import jakarta.persistence.*;

@Entity
@Table(
        name = "messages",
        schema = "public",
        uniqueConstraints = @UniqueConstraint(
                columnNames = {"key", "topic", "partition", "offset"}
        )
)
public class Message {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "key")
    private String key;

    @Column(name = "message", columnDefinition = "TEXT")
    private String message;

    @Column(name = "topic")
    private String topic;

    @Column(name = "partition")
    private Integer partition;

    @Column(name = "offset")
    private Long offset;

    @Column(name = "timestamp")
    private Long timestamp;

    @Column(name = "status")
    private String status;

    public Message() {
        this.status = "NEW";
        this.timestamp = System.currentTimeMillis();
    }

    public Message(String key, String message, String topic, Integer partition, Long offset) {
        this.key = key;
        this.message = message;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.status = "NEW";
        this.timestamp = System.currentTimeMillis();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
