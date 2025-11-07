package com.example.instructions.service;

import com.example.instructions.model.CanonicalTrade;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Collection;

@Component
public class InMemoryStore {
    private final ConcurrentHashMap<String, CanonicalTrade> store = new ConcurrentHashMap<>();

    public void put(CanonicalTrade t) {
        store.put(t.getTradeId(), t);
    }
    public CanonicalTrade get(String id) {
        return store.get(id);
    }
    public void remove(String id) { store.remove(id); }
    public Collection<CanonicalTrade> all() { return store.values(); }
	public Collection<CanonicalTrade> getAll() {
		 return store.values();

	}
}
