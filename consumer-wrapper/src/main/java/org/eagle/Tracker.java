package org.eagle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Tracker {
    private static final  Logger logger = LoggerFactory.getLogger(Tracker.class);

    private final Map<Integer,NavigableSet<Long>> inflightMessages = new HashMap<>();

    public void track(long offset, int partition) {
        inflightMessages.computeIfAbsent(partition, k -> new TreeSet<>()).add(offset);
    }

    public Optional<Long> acknowledge(long offset, int partition) {
        if (inflightMessages.get(partition).contains(offset)) {
            NavigableSet<Long> offsets = inflightMessages.get(partition);
            if (offsets.contains(offset)) {
                return Optional.of(offset+1);
            }
            offsets.remove(offset);
            return Optional.of(offset+1);
        }
        return Optional.empty();
    }

    public void reset() {
        logger.info("Reset tracker");
        inflightMessages.clear();
    }

}
