package com.zqh.spark.connectors.log;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

public class LogGobbler extends Thread {
    private final BufferedReader inputReader;
    private final Logger logger;
    private final Level loggingLevel;
    private final CircularBuffer<String> buffer;

    public LogGobbler(final Reader inputReader, final Logger logger,
                      final Level level, final int bufferLines) {
        this.inputReader = new BufferedReader(inputReader);
        this.logger = logger;
        this.loggingLevel = level;
        buffer = new CircularBuffer<String>(bufferLines);
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                String line = inputReader.readLine();
                if (line == null) {
                    return;
                }
                buffer.append(line);
                log(line);
            }
        } catch (IOException e) {
            error("Error reading from logging stream:", e);
        }
    }

    private void log(String message) {
        if (logger != null) {
            switch (loggingLevel){
                case TRACE:
                    logger.trace(message);
                    break;
                case DEBUG:
                    logger.debug(message);
                    break;
                case INFO:
                    logger.info(message);
                    break;
                case WARN:
                    logger.warn(message);
                    break;
                case ERROR:
                    logger.error(message);
                    break;
            }
        }
    }

    private void error(String message, Exception e) {
        if (logger != null) {
            logger.error(message, e);
        }
    }

    private void info(String message, Exception e) {
        if (logger != null) {
            logger.info(message, e);
        }
    }

    public void awaitCompletion(final long waitMs) {
        try {
            join(waitMs);
        } catch (InterruptedException e) {
            info("I/O thread interrupted.", e);
        }
    }

    public String getRecentLog() {
        return Joiner.on(System.getProperty("line.separator")).join(buffer);
    }

}