package io.github.majusko.pulsar.producer;

import io.github.majusko.pulsar.error.exception.TopicNotCreateException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class PulsarTemplate<T> {

    private final ProducerCollector producerCollector;

    public PulsarTemplate(ProducerCollector producerCollector) {
        this.producerCollector = producerCollector;
    }

    public MessageId send(String topic, T msg) throws PulsarClientException {
        //noinspection unchecked
        Producer<T> producer = producerCollector.getProducer(topic);
        if (producer != null) {
            return producer.send(msg);
        }
        throw new TopicNotCreateException("topic = [" + topic + "] not create in ProducerFactory");
    }

    public CompletableFuture<MessageId> sendAsync(String topic, T message) {
        Producer producer = producerCollector.getProducer(topic);
        if (producer != null) {
            return producer.sendAsync(message);
        }
        throw new TopicNotCreateException("topic = [" + topic + "] not create in ProducerFactory");
    }

    public TypedMessageBuilder<T> createMessage(String topic, T message) {
        Producer producer = producerCollector.getProducer(topic);
        if (producer != null) {
            return producer.newMessage().value(message);
        }
        throw new TopicNotCreateException("topic = [" + topic + "] not create in ProducerFactory");
    }
}
