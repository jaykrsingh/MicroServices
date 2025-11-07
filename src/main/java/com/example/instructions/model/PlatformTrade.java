package com.example.instructions.model;

import java.util.Objects;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PlatformTrade {
    private String platformId;
    private Trade trade;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Trade {
        private String account;
        private String security;
        private String type;
        private double amount;
        private String timestamp;
    }

	public String getPlatformId() {
		return platformId;
	}



	public Trade getTrade() {
		return trade;
	}

	public void setTrade(Trade trade) {
		this.trade = trade;
	}

	@Override
	public int hashCode() {
		return Objects.hash(platformId, trade);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PlatformTrade other = (PlatformTrade) obj;
		return Objects.equals(platformId, other.platformId) && Objects.equals(trade, other.trade);
	}

	public void setPlatformId(String platformId) {
		this.platformId = platformId;
		
	}
}