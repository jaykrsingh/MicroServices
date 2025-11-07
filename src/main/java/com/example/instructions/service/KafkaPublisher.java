package com.example.instructions.service;


import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import com.example.instructions.model.PlatformTrade.Trade;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaPublisher {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<Void> publish(CanonicalTrade ct) {
        try {
            PlatformTrade pt = mapToPlatform(ct);
            String payload = objectMapper.writeValueAsString(pt);
            // async publish to instructions.outbound, key = trade id
            return kafkaTemplate.send("instructions.outbound", ct.getTradeId(), payload)
                    
                    .thenApply(recordMetadata -> null);
        } catch (Exception e) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    private PlatformTrade mapToPlatform(CanonicalTrade ct) {
        PlatformTrade pt = new PlatformTrade();
        pt.setPlatformId("ACCT123");    //setPlatformId("ACCT123");
        Map<String, Object> trade = Map.of(
                "account", TradeSafe.mask(ct.getAccountNumber()),
                "security", ct.getSecurityId(),
                "type", ct.getTradeType(),
                "amount", ct.getAmount(),
                "timestamp", ct.getTimestamp().toString()
        );
        pt.setTrade((Trade) trade);     //setTrade(trade);
        return pt;
    }

    // small internal helper to avoid coupling with transformer
    private static class TradeSafe {
        static String mask(String acct) {
            if (acct == null) return null;
            String digits = acct.replaceAll("\\D", "");
            int len = digits.length();
            if (len <= 4) return "****" + digits;
            return "****" + digits.substring(len - 4);
        }
    }
}