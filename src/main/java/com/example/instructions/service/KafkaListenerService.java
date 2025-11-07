package com.example.instructions.service;



import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.util.TradeTransformer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class KafkaListenerService {
    private final InMemoryStore store;
    private final KafkaPublisher publisher;
    private final ObjectMapper mapper;

    public KafkaListenerService(InMemoryStore store, KafkaPublisher publisher, ObjectMapper mapper) {
        this.store = store;
        this.publisher = publisher;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "instructions.inbound", groupId = "instructions-capture")
    public void onMessage(ConsumerRecord<String, String> record) {
        try {
            String json = record.value();
            JsonNode node = mapper.readTree(json);

            // assume inbound JSON fields: accountNumber, securityId, tradeType, amount, timestamp
            String account = node.path("accountNumber").asText(null);
            String security = node.path("securityId").asText(null);
            String type = node.path("tradeType").asText(null);
            long amount = node.path("amount").asLong(0);
            Instant ts = node.has("timestamp") ? Instant.parse(node.get("timestamp").asText()) : Instant.now();
            String st = ts.toString();
            CanonicalTrade ct = TradeTransformer.fromRaw(account, security, type, amount, st);
            store.put(ct);

            // async publish
            publisher.publish(ct).whenComplete((v, ex) -> {
                if (ex == null) {
                    store.remove(ct.getTradeId()); // remove on success (or move to audit)
                } else {
                    // keep in store for retry logic; log sanitized info
                    System.err.println("Publish failed for trade id=" + ct.getTradeId());
                }
            });
        } catch (Exception e) {
            // don't log raw message that may contain sensitive info
            System.err.println("Failed to process inbound kafka message: " + e.getMessage());
        }
    }
}