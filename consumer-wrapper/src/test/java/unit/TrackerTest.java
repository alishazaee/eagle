package unit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.shahin.Tracker;


import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class TrackerTest {
    private Tracker tracker;

    @BeforeEach
    void setUp(){
        tracker = new Tracker();
    }

    @Test
    void testAddOffsetInTheSamePartition(){
        tracker.track(10L,10);
        tracker.track(11L,10);

        Optional<Long> ack = tracker.acknowledge(11L,10);
        assertEquals(12L, (long) ack.get(), "Tracker did not ack the correct offset for the same partition");
    }

    @Test
    void testAddOffsetInDifferentPartition(){
        tracker.track(10L,5);
        tracker.track(11L,6);

        Optional<Long> ack = tracker.acknowledge(10L,5);
        assertTrue(ack.isPresent(),"Tracker returned empty Optional");
        assertEquals(11L ,(long)ack.get(), "Tracker did not ack the correct for partition 5");
    }


}