package ru.mai.lessons.rpks.impl;

import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.ILineFinder;
import ru.mai.lessons.rpks.exception.LineCountShouldBePositiveException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class LineFinder implements ILineFinder {
    private String GetFilePath(String filename) {
        return Objects.requireNonNull(getClass().getClassLoader().getResource(".")).getPath() + filename;
    }
    @Override
    public void find(String inputFilename, String outputFilename, String keyWord, int lineCount) throws LineCountShouldBePositiveException {
        inputFilename = GetFilePath(inputFilename);
        outputFilename = GetFilePath(outputFilename);
        if (lineCount < 0) {
            log.error("wrong lineCount parameter ");
            throw new LineCountShouldBePositiveException("lineCount cannot be < 0 !");
        }
        int threadCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executors = Executors.newFixedThreadPool(threadCount);
        File inputFile = new File(inputFilename);
        File outputFile = new File(outputFilename);
        long fileSize = inputFile.length();
        short chunkSize = 2048;
        long keyWordLength = keyWord.length();
        long countChunks = fileSize / chunkSize;
        ConcurrentNavigableMap<Long, Future<List<String>>> resultFuturesMap = new ConcurrentSkipListMap<>();
        String finalInputFilename = inputFilename;
        for (int i = 0; i < countChunks; i++) {
            long start = i * (chunkSize - keyWordLength);
            long end = (i == countChunks - 1) ? fileSize : start + chunkSize;
            resultFuturesMap.put(start, executors.submit(() -> threadTask(finalInputFilename, start, end, keyWord, lineCount)));
        }
        executors.shutdown();
        try {
            if (!executors.awaitTermination(1, TimeUnit.MINUTES)) {
                log.error("we were waiting for 1 minute and it didnt finish");
            }
        } catch (InterruptedException e) {
            log.error("some thread was interrupted " + e.getMessage());
            throw new RuntimeException(e);
        }
        write(outputFile, resultFuturesMap);
    }

    private List<String> threadTask(String inputFilename, Long start, Long end, String keyWord, int lineCount) throws IOException {
        Optional<List<String>> stringList = Optional.of(new ArrayList<>());
        try (RandomAccessFile raf = new RandomAccessFile(new File(inputFilename), "r")) {
            raf.seek(end);
            long endSegment = end;
            while (raf.getFilePointer() < raf.length()
                    && !Character.isWhitespace(raf.readByte())) {
                endSegment++;
            }
            raf.seek(start);
            byte[] buffer = new byte[(int) (endSegment - start)];
            raf.read(buffer);
            String chunk = new String(buffer, StandardCharsets.UTF_8);
            ByteBuffer byteBuffer = ByteBuffer.wrap(chunk.toLowerCase().getBytes(StandardCharsets.UTF_8));
            byte[] keyWordBytes = keyWord.getBytes(StandardCharsets.UTF_8);
            byte[] currentBytes;
            TreeMap<Long, Long> mapIndexes = new TreeMap<>();
            byte[] successfulPart;
            int localEnd;
            for (int i = 0; i < buffer.length - keyWordBytes.length; i++) {
                currentBytes = new byte[keyWordBytes.length];
                byteBuffer.get(currentBytes);
                if (Arrays.equals(currentBytes, keyWordBytes)) {
                    mapIndexes.put(getLeftBorder(raf, i + start, lineCount),
                            getRightBorder(raf, i + start, lineCount));
                }
                byteBuffer.position(byteBuffer.position() - keyWordBytes.length + 1);
            }
            for (var key : mapIndexes.keySet()) {
                localEnd = mapIndexes.get(key).intValue();
                successfulPart = new byte[localEnd - key.intValue()];
                raf.seek(key);
                raf.read(successfulPart);
                String finalSuccessfullString = new String(successfulPart, StandardCharsets.UTF_8);
                stringList.ifPresent(list -> list.add(finalSuccessfullString));
            }
        } catch (IOException ex) {
            log.error("Error reading file: " + ex.getMessage());
            throw ex;
        }
        return stringList.orElse(Collections.emptyList());
    }
    private static void write(File outputFile, ConcurrentNavigableMap<Long, Future<List<String>>> resultMap){
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile))) {
            for (var key : resultMap.keySet()) { //удаляем пары, где было 0 подходящих строк в чанке
                var value = resultMap.get(key);
                if (!value.isDone()) {
                    log.error("future is still being unexecuted");
                    throw new RuntimeException("future is still being unexecuted");
                }
                if (value.get().isEmpty()) {
                    resultMap.remove(key);
                }
            }
            for (var key : resultMap.keySet()) {
                var value = resultMap.get(key);
                List<String> stringList = value.get();
                String currentString;
                String lastString;
                for (int i = 0; i < stringList.size(); i ++) {
                    currentString = stringList.get(i);
                    if (Objects.equals(key, resultMap.lastKey())) { //последний список строк
                        if (i == stringList.size() - 1
                                && (currentString.toCharArray()[currentString.length() - 1] == '\n')) { //последняя строка в нем, в конце которой перенос строки
                            lastString = currentString.substring(0, currentString.length()  - 1);
                            bufferedWriter.write(lastString);
                        } else {
                            bufferedWriter.write(currentString);
                        }
                    } else  {
                        bufferedWriter.write(currentString);
                    }
                }
            }
        } catch (Exception e) {
            log.error("cannot write data in file!");
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private long getLeftBorder(RandomAccessFile raf, long positionKeyword, int lineCount) throws IOException {
        long leftOffset = 0;
        int countLines = 0;
        raf.seek(positionKeyword);
        long findPosition = raf.getFilePointer();
        while (leftOffset < findPosition && countLines <= lineCount) {
            if (raf.readByte() == '\n') {
                countLines++;
            }
            leftOffset++;
            raf.seek(findPosition - leftOffset);
        }
        if (leftOffset < findPosition) {
            leftOffset -= 2;
            raf.seek(findPosition - leftOffset);
        }
        return raf.getFilePointer();
    }
    private long getRightBorder(RandomAccessFile raf, long positionKeyword, int lineCount) throws IOException {
        int countLines = 0;
        int rightOffset = 0;
        raf.seek(positionKeyword);
        long possibleOffset = raf.length() - positionKeyword;
        while (rightOffset < possibleOffset && countLines <= lineCount) {
            if (raf.readByte() == '\n') {
                countLines++;
            }
            rightOffset++;
        }
        return raf.getFilePointer();
    }
}



