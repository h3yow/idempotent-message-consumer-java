package com.github.h3yow.idempotentmessageconsumerjava.mq.store;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * 基于单个本地 JSON 文件的存储，用来模拟消息中间件的持久化与“服务重启恢复”：
 * broker 关闭后重新构造一个实例并指向同一文件，即可读回全部消息、位点、尝试与死信状态。
 *
 * <p>写入采用“先写临时文件再原子移动”，避免半写文件污染状态。
 */
public final class FileStateStore implements StateStore {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

    private final Path file;

    public FileStateStore(Path file) {
        this.file = file;
    }

    @Override
    public synchronized BrokerState load() {
        if (!Files.exists(file)) {
            BrokerState fresh = new BrokerState();
            save(fresh);
            return fresh;
        }
        try {
            return MAPPER.readValue(file.toFile(), BrokerState.class);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load broker state from " + file, e);
        }
    }

    @Override
    public synchronized void save(BrokerState state) {
        try {
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), state);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to save broker state to " + file, e);
        }
    }
}
