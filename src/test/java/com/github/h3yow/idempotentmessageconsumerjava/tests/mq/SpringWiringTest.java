package com.github.h3yow.idempotentmessageconsumerjava.tests.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.IdempotentBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.store.InMemoryStateStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.store.StateStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@TestPropertySource(properties = "idempotent-mq.state-file=")
class SpringWiringTest {

    @Autowired
    IdempotentBroker broker;

    @Autowired
    StateStore stateStore;

    @Test
    void brokerAndStoreAreAutoConfigured() {
        assertNotNull(broker, "IdempotentBroker bean must be auto-configured");
        assertNotNull(stateStore);
        org.junit.jupiter.api.Assertions.assertTrue(stateStore instanceof InMemoryStateStore,
                "blank state-file should yield in-memory store");
        System.out.println("[SPRING] broker=" + broker.getClass().getSimpleName()
                + ", store=" + stateStore.getClass().getSimpleName());
    }
}
