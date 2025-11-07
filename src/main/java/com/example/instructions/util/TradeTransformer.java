package com.example.instructions.util;


import com.example.instructions.model.CanonicalTrade;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public class TradeTransformer {
    private static final Pattern SECURITY_PATTERN = Pattern.compile("^[A-Z0-9\\-]{1,12}$");
    public static String maskAccount(String acct) {
        if (acct == null) return null;
        String digits = acct.replaceAll("\\D", "");
        int len = digits.length();
        if (len <= 4) return "****" + digits;
        return "****" + digits.substring(len - 4);
    }

    public static String normalizeSecurity(String sec) {
        if (sec == null) return null;
        String up = sec.trim().toUpperCase();
        if (!SECURITY_PATTERN.matcher(up).matches()) throw new IllegalArgumentException("Invalid security id");
        return up;
    }

    public static String normalizeTradeType(String type) {
        if (type == null) return null;
        String t = type.trim().toLowerCase();
        return switch (t) {
            case "buy", "b" -> "B";
            case "sell", "s" -> "S";
            default -> throw new IllegalArgumentException("Unsupported trade type: " + type);
        };
    }

    public static CanonicalTrade fromRaw(String account, String security, String type, long amount, String ts) {
        CanonicalTrade ct = new CanonicalTrade();
        
        ct.setTradeId(UUID.randomUUID().toString());
       
        ct.setAccountNumber(account); // keep raw (but don't log)
        ct.setSecurityId(normalizeSecurity(security));
        ct.setTradeType(normalizeTradeType(type));
        ct.setAmount(amount);
        ct.setTimestamp(ts);
        return ct;
    }  
    
    /**
     * Converts CanonicalTrade into platform-specific JSON structure.
     */
    public static Map<String, Object> toPlatformTrade(CanonicalTrade ct) {
        return Map.of(
            "platform_id", "ACCT123",
            "trade", Map.of(
                "account", maskAccount(ct.getAccountNumber()),
                "security", ct.getSecurityId(),
                "type", ct.getTradeType(),
                "amount", ct.getAmount(),
                "timestamp", ct.getTimestamp().toString()
            )
        );
    }
    
    public static CanonicalTrade parseToCanonical(Map<String, Object> rawTrade) {
        CanonicalTrade ct = new CanonicalTrade();

        // Unique ID for traceability
        ct.setTradeId(UUID.randomUUID().toString());

        // Normalize and validate account number
        String accountNumber = String.valueOf(rawTrade.get("account_number"));
        ct.setAccountNumber(maskAccount(accountNumber));

        // Normalize and validate security ID (uppercase)
        String securityId = String.valueOf(rawTrade.get("security_id")).toUpperCase();
        ct.setSecurityId(securityId);

        // Normalize trade type ("Buy" → "B", "Sell" → "S")
        String tradeType = normalizeTradeType(String.valueOf(rawTrade.get("trade_type")));
        ct.setTradeType(tradeType);

        // Parse amount
        double amount = Double.parseDouble(String.valueOf(rawTrade.get("amount")));
        ct.setAmount(amount);

        // Parse timestamp (or assign current time if missing)
        Object ts = rawTrade.get("timestamp");
        Instant instant = ts != null ? Instant.parse(ts.toString()) : Instant.now();
        String st = instant.toString();
        ct.setTimestamp(st);

        return ct;
    }
    
    


}
