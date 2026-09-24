package com.github.h3yow.idempotentmessageconsumerjava.mq.broker;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * 基于单个 JSON 文件的 {@link BrokerStore}：每次全量序列化后先写 {@code .tmp} 再原子 rename，
 * 任何时刻正式文件要么是旧版本、要么是新版本，不会出现半写入内容。
 *
 * <p>正式文件不存在时 {@link #load()} 返回空状态，模拟“全新中间件首次启动”。</p>
 */
public class FileBrokerStore implements BrokerStore {

    private final Path file;
    private final ObjectMapper mapper;

    public FileBrokerStore(Path file) {
        this(file, new ObjectMapper());
    }

    public FileBrokerStore(Path file, ObjectMapper mapper) {
        this.file = file;
        this.mapper = mapper;
    }

    @Override
    public synchronized void persist(BrokerState state) {
        try {
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            byte[] json = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(state);
            Files.write(tmp, json,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
            try {
                Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicNotSupported) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist broker state to " + file, e);
        }
    }

    @Override
    public synchronized BrokerState load() {
        if (!Files.exists(file)) {
            return new BrokerState();
        }
        try {
            return mapper.readValue(file.toFile(), BrokerState.class);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load broker state from " + file, e);
        }
    }
}
