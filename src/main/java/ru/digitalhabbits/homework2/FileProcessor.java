package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    private final ExecutorService executorService = Executors.newFixedThreadPool(CHUNK_SIZE);
    private final ExecutorService executorServiceForWriter = Executors.newSingleThreadExecutor();

    private final LineProcessor<Integer> lineCounter = new LineCounterProcessor();

    private final Exchanger<Pair<String, Integer>> exchanger = new Exchanger<>();


    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);

        // запускаем FileWriter в отдельном потоке
        FileWriter fileWriter = new FileWriter(exchanger, resultFileName);
        executorServiceForWriter.execute(fileWriter);

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {

            while (scanner.hasNext()) {
                // вычитываем CHUNK_SIZE строк для параллельной обработки
                List<String> tempList = new ArrayList<>(CHUNK_SIZE);

                for (int i = 0; i < CHUNK_SIZE; i++) {
                    if (scanner.hasNext()) {
                        tempList.add(scanner.nextLine());
                    }
                }
                // обрабатываем строку с помощью LineProcessor. Каждый поток обрабатывает свою строку.
                List<Future<Pair<String, Integer>>> futureList = tempList
                        .stream()
                        .map(line -> executorService.submit(() -> lineCounter.process(line)))
                        .collect(Collectors.toList());

                // передаём обработанные данные в поток, записывающий данные в результирующий файл
                for (Future<Pair<String, Integer>> pairFuture : futureList) {
                    // future.get() блокирует выполнение до тех пор, пока задача не будет завершена
                    exchanger.exchange(pairFuture.get());
                }
            }

        } catch (IOException | InterruptedException | ExecutionException exception) {
            logger.error("", exception);
        }

        // остановливаем поток writer
        executorService.shutdown();
        executorServiceForWriter.shutdownNow();

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
}
