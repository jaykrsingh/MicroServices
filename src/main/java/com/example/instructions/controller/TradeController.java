
package com.example.instructions.controller;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.service.InMemoryStore;
import com.example.instructions.service.KafkaPublisher;
import com.example.instructions.util.TradeTransformer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/trades")
public class TradeController {
    private final ObjectMapper mapper;
    private final InMemoryStore store;
    private final KafkaPublisher publisher;

    public TradeController(ObjectMapper mapper, InMemoryStore store, KafkaPublisher publisher) {
        this.mapper = mapper;
        this.store = store;
        this.publisher = publisher;
    }

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> uploadFile(@RequestPart("file") MultipartFile file) throws IOException {
        String filename = file.getOriginalFilename();
        if (!StringUtils.hasText(filename)) return ResponseEntity.badRequest().body("Missing file name");
        String ext = filename.substring(filename.lastIndexOf('.') + 1).toLowerCase();

        List<String> ids = new ArrayList<>();
        if ("csv".equals(ext)) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
                String header = br.readLine();
                if (header == null) return ResponseEntity.badRequest().body("Empty file");

                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",", -1);
                    // expected order: accountNumber, securityId, tradeType, amount, timestamp
                    if (parts.length < 5) continue;
                    String account = parts[0].trim();
                    String security = parts[1].trim();
                    String type = parts[2].trim();
                    long amount = Long.parseLong(parts[3].trim());
                    Instant ts = Instant.parse(parts[4].trim());
                    String st = ts.toString();
                    CanonicalTrade ct = TradeTransformer.fromRaw(account, security, type, amount, st);
                    store.put(ct);
                    ids.add(ct.getTradeId());
                    publisher.publish(ct);
                }
            }
        } else if ("json".equals(ext)) {
            // assume newline-delimited JSON or array
            try (InputStream input = file.getInputStream()) {
                JsonNode root = mapper.readTree(input);
                if (root.isArray()) {
                    for (JsonNode node : root) {
                        processJsonNode(node, ids);
                    }
                } else if (root.isObject()) {
                    // single or ndjson style (object per line not handled here)
                    processJsonNode(root, ids);
                } else {
                    return ResponseEntity.badRequest().body("Unsupported JSON structure");
                }
            }
        } else {
            return ResponseEntity.badRequest().body("Unsupported file type: " + ext);
        }

        return ResponseEntity.ok().body(Map.of("ingested", ids.size(), "ids", ids));
    }

    private void processJsonNode(JsonNode node, List<String> ids) {
        String account = node.path("accountNumber").asText(null);
        String security = node.path("securityId").asText(null);
        String type = node.path("tradeType").asText(null);
        long amount = node.path("amount").asLong(0);
        Instant ts = node.has("timestamp") ? Instant.parse(node.get("timestamp").asText()) : Instant.now();
        String st = ts.toString();
        CanonicalTrade ct = TradeTransformer.fromRaw(account, security, type, amount, st);
        store.put(ct);
        ids.add(ct.getTradeId());
        publisher.publish(ct);
    }

    @GetMapping("/store")
    public ResponseEntity<?> listStore() {
        // Return sanitized entries for auditing (mask account)
        var sanitized = store.all().stream().map(ct -> Map.of(
                "id", ct.getTradeId(),
                "account", TradeTransformer.maskAccount(ct.getAccountNumber()),
                "security", ct.getSecurityId(),
                "type", ct.getTradeType(),
                "amount", ct.getAmount(),
                "timestamp", ct.getTimestamp().toString()
        )).toList();
        return ResponseEntity.ok(sanitized);
    }
}