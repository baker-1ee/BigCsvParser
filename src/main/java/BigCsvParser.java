import com.opencsv.bean.CsvToBeanBuilder;
import io.micrometer.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

@Slf4j
public class BigCsvParser<T extends CsvMappingClass> {
    private static final int DEFAULT_CHUNK_SIZE = 10000;
    private final int CSV_FIRST_DATA_ROW_NUMBER = 2;
    private final int DEFAULT_START_ROW_NUMBER = CSV_FIRST_DATA_ROW_NUMBER;
    private final String TEMP_FILE_PATH = "x-storage/tmp";
    private final Class<T> tClass;
    private final int chunkSize;

    private List<T> chunkList;
    private int currentRowNumber;

    public BigCsvParser(Class<T> clazz) throws CsvParserException {
        this(clazz, DEFAULT_CHUNK_SIZE);
    }

    public BigCsvParser(Class<T> clazz, int chunkSize) throws CsvParserException {
        tClass = clazz;
        if (chunkSize <= 0) {
            throw new CsvParserException("chunk size 는 양수만 입력 가능");
        }
        this.chunkSize = chunkSize;
        chunkList = new ArrayList<>(this.chunkSize);
    }

    /**
     * 파일을 객체로 파싱하여 chunk 단위로 작업을 수행한다.
     */
    public void processByChunk(MultipartFile multipartFile, Consumer<List<T>> processor) throws CsvParserException {
        processByChunk(multipartFile, processor, DEFAULT_START_ROW_NUMBER);
    }

    /**
     * 파일을 객체로 파싱하여 chunk 단위로 작업을 수행한다.
     * 사용자가 원하는 시작행부터 시작한다.
     */
    public void processByChunk(MultipartFile multipartFile, Consumer<List<T>> processor, int startRowNumber) throws CsvParserException {
        currentRowNumber = CSV_FIRST_DATA_ROW_NUMBER;
        File tempFile = null;

        try {
            tempFile = makeTempFile(multipartFile);
            Iterator<T> csvToObjectParser = new CsvToBeanBuilder<T>(getReader(tempFile))
                    .withType(tClass)
                    .withSkipLines(OpenCsvBindColumnEnum.getSkipLine(tClass))
                    .withIgnoreLeadingWhiteSpace(true)
                    .build()
                    .iterator();

            // 파싱 시작행까지 스킵
            while (currentRowNumber < startRowNumber && csvToObjectParser.hasNext()) {
                csvToObjectParser.next();
                ++currentRowNumber;
            }
            // 파싱 및 후처리 작업 수행
            while (csvToObjectParser.hasNext()) {
                collectParsedObjectToChunkList(csvToObjectParser);
                if (isChunkListFull()) {
                    work(processor, startRowNumber);
                }
            }
            work(processor, startRowNumber);

        } catch (IOException e) {
            handleException("BigCsvParser 초기화 작업 중 예외 발생", e);
        } catch (CsvParserAfterProcessException e) {
            handleException(e.getMessage(), e);
        } catch (Exception e) {
            handleException(String.format("%d행 파싱 작업 중 예외 발생", currentRowNumber), e);
        } finally {
            deleteTempFile(tempFile);
        }
    }

    // csv data 를 객체로 파싱해서 chunk list 에 수집
    private void collectParsedObjectToChunkList(Iterator<T> csvToObjectParser) {
        ++currentRowNumber;
        T obj = csvToObjectParser.next();
        chunkList.add(obj);
    }

    // chunk list 가 가득 찼다면 true
    private boolean isChunkListFull() {
        return chunkList.size() == chunkSize;
    }

    // chunk 단위로 요청 받은 후처리 작업 수행
    private void work(Consumer<List<T>> processor, int startRowNumber) throws CsvParserAfterProcessException {
        try {
            processor.accept(chunkList);
            chunkList = new ArrayList<>(chunkSize);
        } catch (Exception e) {
            int workStartRowNumber = currentRowNumber >= chunkSize ? currentRowNumber - chunkSize : startRowNumber;
            int workEndRowNumber = currentRowNumber - 1;
            String message = String.format("%d행부터 %d행까지의 데이터에 대해 후처리 작업 중 예외 발생", workStartRowNumber, workEndRowNumber);
            throw new CsvParserAfterProcessException(message, e);
        }
    }

    private void handleException(String message, Exception e) throws CsvParserException {
        log.error(message, e);
        throw new CsvParserException(message, e);
    }

    private BufferedReader getReader(File tempFile) throws FileNotFoundException {
        return new BufferedReader(
                new InputStreamReader(
                        new BOMInputStream(new FileInputStream(tempFile), ByteOrderMark.UTF_8),
                        StandardCharsets.UTF_8
                )
        );
    }

    // multipart file 을 file 로 변환
    private File makeTempFile(MultipartFile multipartFile) throws IOException {
        String tempFileName = getTempFileName(multipartFile);
        String tempFileAbsolutePath = Paths.get(TEMP_FILE_PATH).toAbsolutePath().normalize().toString();
        return convertToFile(multipartFile, tempFileAbsolutePath, tempFileName);
    }

    // csv 파싱 목적의 임시 파일 생성을 위한 파일명
    private String getTempFileName(MultipartFile multipartFile) {
        String fileName = StringUtils.isNotEmpty(multipartFile.getOriginalFilename()) ? multipartFile.getOriginalFilename() : multipartFile.getName();
        String extension = fileName.substring(fileName.lastIndexOf("."));
        UUID uuid = UUID.randomUUID();
        return uuid + extension;
    }

    private File convertToFile(MultipartFile multipartFile, String path, String name) throws IOException {
        if (StringUtils.isEmpty(name)) {
            name = multipartFile.getOriginalFilename();
        }

        File file = new File(path + "/" + name);
        multipartFile.transferTo(file);

        return file;
    }

    // 임시 파일 삭제
    private void deleteTempFile(File tempFile) {
        if (tempFile != null && tempFile.exists()) {
            boolean isDeleted = tempFile.delete();
            log.info("temp file was deleted : {}", isDeleted);
        }
    }
}
