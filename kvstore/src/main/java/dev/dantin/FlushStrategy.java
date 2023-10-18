package dev.dantin;

import java.io.BufferedWriter;

public interface FlushStrategy {

    default void doWithWriter(BufferedWriter writer) {

    }
}
