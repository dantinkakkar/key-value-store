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

public abstract class AbstractKeyStore {
    protected Map<String, Pair<Long, String>> underlyingStore;
    protected BufferedWriter writer;
    private ScheduledExecutorService flushService;
    private final long flushDuration;
    private static final int MAX_APPEND_COUNT = 30000;
    protected int appendCount;

    public AbstractKeyStore(
            final Map<String, Pair<Long, String>> underlyingStoreImpl,
            final long flushDuration
    ) throws IOException {
        this.underlyingStore = underlyingStoreImpl;
        this.appendCount = 0;
        this.flushDuration = flushDuration;
        initWalWriter();
    }

    synchronized public void writeValue(String key, String value, long time) throws IOException {
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
            new Thread(() -> {
                try {
                    writeToSSTable(underlyingStore);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).start();
            initWalWriter();
        }
    }

    public String getValue(String key) {
        return underlyingStore.get(key).getValue();
    }

    public void close() throws IOException {
        writer.close();
        flushService.close();
    }

    private static void writeToSSTable(final Map<String, Pair<Long, String>> _store) throws IOException {
        final BufferedWriter fileWriter = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(
                Path.of("sstable_"+System.currentTimeMillis()),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.SYNC
        )), 1024*1024*30);
        final List<String> sortedKeys = _store.keySet().stream().sorted().toList();
        for (String k: sortedKeys) {
            fileWriter.write(k+":"+_store.get(k).getValue()+":"+_store.get(k).getKey());
            fileWriter.newLine();
        }
        fileWriter.close();
    }

    protected void initWalWriter() throws IOException {
        File wal = new File("store");
        wal.delete();
        wal.createNewFile();
        final long bufSize = flushDuration * 3140000 < 0 ? Integer.MAX_VALUE - 8 : flushDuration * 3140000;
        writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(
                Path.of("store"), StandardOpenOption.APPEND, StandardOpenOption.DSYNC
        )), (int) bufSize);
        underlyingStore = new ConcurrentHashMap<>();
        appendCount = 0;
        if (flushService != null) flushService.shutdownNow();
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
