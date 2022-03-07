package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Exchanger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);

    public static final String FORMAT = "%s %d\n";

    private final Exchanger<Pair<String, Integer>> exchanger;
    private final Path resultFile;

    public FileWriter(Exchanger<Pair<String, Integer>> exchanger, @Nonnull String outputFile) {
        this.exchanger = exchanger;
        this.resultFile = createFile(outputFile);
    }

    private Path createFile(String outputFile) {
        Path path = Path.of(outputFile);
        try {
            if (Files.notExists(path)) {
                return Files.createFile(path);
            } else return path;
        } catch (IOException e) {
            logger.error("", e);
            return path;
        }
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Pair<String, Integer> pair = exchanger.exchange(null);
                writeLine(pair);
            } catch (IOException | InterruptedException e) {
                logger.error("", e);
            }
        }
        logger.info("Finish writer thread {}", currentThread().getName());
    }

    private void writeLine(Pair<String, Integer> line) throws IOException {
        Files.writeString(resultFile,
                String.format(FORMAT, line.getKey(), line.getValue()),
                StandardOpenOption.APPEND);
    }
}
