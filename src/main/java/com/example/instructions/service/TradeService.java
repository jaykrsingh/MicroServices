package com.example.instructions.service;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import com.example.instructions.util.TradeTransformer;

import ch.qos.logback.classic.Logger;
import lombok.extern.slf4j.Slf4j;

import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Service
public class TradeService {
	
    private final KafkaPublisher kafkaPublisher;
    private final ConcurrentHashMap<String, CanonicalTrade> inMemoryStore = new ConcurrentHashMap<>();
    private final InMemoryStore store;
    
    public TradeService(InMemoryStore store, KafkaPublisher kafkaPublisher) {
        this.store = store;
        this.kafkaPublisher = kafkaPublisher;
    }
    
    private Map<String, Object> parseCsvLine(String line) {
        Map<String, Object> map = new HashMap<>();
        if (line == null || line.isBlank()) {
            System.out.println("Skipping empty CSV line");
            return map;
        }

        // split with -1 to preserve empty trailing fields
        String[] parts = line.split(",", -1);

        // defensive: trim and guard against missing columns
        String account = parts.length > 0 ? parts[0].trim() : "";
        String security = parts.length > 1 ? parts[1].trim() : "";
        String type = parts.length > 2 ? parts[2].trim() : "";
        String amountStr = parts.length > 3 ? parts[3].trim() : "0";
        String timestamp = parts.length > 4 ? parts[4].trim() : null;

        // try to parse amount to a numeric type; TradeTransformer.parseToCanonical will convert again if needed
        double amount = 0.0;
        try {
            if (!amountStr.isEmpty()) amount = Double.parseDouble(amountStr);
        } catch (NumberFormatException nfe) {
        	System.out.println("Invalid amount '{}' in line: {}. Defaulting to 0");
        }

        map.put("account_number", account);
        map.put("security_id", security);
        map.put("trade_type", type);
        map.put("amount", amount);
        map.put("timestamp", timestamp);

        return map;
    }
    
    private void storeInMemory(CanonicalTrade ct) {
        try {
            store.put(ct);
        } catch (Exception e) {
        	System.out.println("Failed to store trade in memory id={}");
        }
    }
    


    public void processFile(MultipartFile file) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {

        	reader.lines()
            .skip(1)
            .map(this::parseCsvLine)
            .map(TradeTransformer::parseToCanonical)
            .peek(this::storeInMemory)
            .forEach(kafkaPublisher::publish); 

        } catch (Exception e) {
        	System.out.println("❌ Error processing file");
        }
    }
        
        
    }