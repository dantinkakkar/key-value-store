package dev.dantin;

import javafx.util.Pair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KeyValueStore {

    private Map<String, Pair<Long, String>> underlyingStore;
    protected BufferedWriter writer;
    private ScheduledExecutorService flushService;
    private final long flushDuration;
    private static final int MAX_APPEND_COUNT = 30000;
    private int appendCount;

    public KeyValueStore(
            final Map<String, Pair<Long, String>> underlyingStoreImpl,
            final long flushDuration
    ) throws IOException {
        this.underlyingStore = underlyingStoreImpl;
        this.appendCount = 0;
        this.flushDuration = flushDuration;
        initWalWriter();
    }

    synchronized public void writeValue(String key, String value, long time) {
        underlyingStore.compute(key, (__, oldValue) -> {
            final long oldTime = oldValue != null ? oldValue.getKey() : 0;
            try {
                writer.write(key + ":" + value + ":" + time);
                writer.newLine();
                doWithWriter(writer);
                appendCount++;
                return time > oldTime ? new Pair<>(time, value) : oldValue;
            } catch (IOException e) {
                e.printStackTrace();
                return oldValue;
            }
        });
        if (appendCount == MAX_APPEND_COUNT) {
            try {
                writeToSSTable();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String getValue(String key) {
        return underlyingStore.get(key).getValue();
    }

    public void close() throws IOException {
        writer.close();
        flushService.close();
    }

    private void writeToSSTable() throws IOException {
        if (appendCount == MAX_APPEND_COUNT) {
            final BufferedWriter fileWriter = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(
                    Path.of("sstable_"+System.currentTimeMillis()),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.SYNC
            )), 1024*1024*30);
            final List<String> sortedKeys = underlyingStore.keySet().stream().sorted().toList();
            for (String k: sortedKeys) {
                fileWriter.write(k+":"+underlyingStore.get(k).getValue()+":"+underlyingStore.get(k).getKey());
                fileWriter.newLine();
            }
            fileWriter.close();
            underlyingStore = new ConcurrentHashMap<>();
            writer.close();
            File wal = new File("store");
            wal.delete();
            wal.createNewFile();
            initWalWriter();
            appendCount = 0;
            underlyingStore = new ConcurrentHashMap<>();
        }
    }

    protected void initWalWriter() throws IOException {
        final long bufSize = flushDuration * 3140000 < 0 ? Integer.MAX_VALUE - 8 : flushDuration * 3140000;
        if (flushService != null) flushService.shutdownNow();
        writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(
                Path.of("store"), StandardOpenOption.APPEND, StandardOpenOption.DSYNC
        )), (int) bufSize);
        flushService = Executors.newSingleThreadScheduledExecutor();
        flushService.scheduleAtFixedRate(new Thread(() -> {
            try {
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }), 0, flushDuration, TimeUnit.MILLISECONDS);
    }

    protected void doWithWriter(BufferedWriter writer) { }
}
