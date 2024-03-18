import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvBindByPosition;
import com.opencsv.bean.CsvCustomBindByName;
import com.opencsv.bean.CsvCustomBindByPosition;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.mock.web.MockMultipartFile;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class BigCsvParserTest {

    private final String TEST_FILE_NAME = "sample_scraping_data.csv";
    private final String EXCEPTION_TEST_FILE_NAME = "invalid_scraping_data.csv";

    @Test
    @DisplayName("csv 컬럼명으로 파싱하여 원하는 작업을 수행하는 예시")
    void howToUseBigCsvParserByColumnName() throws IOException, CsvParserException {
        // given
        MockMultipartFile mockMultipartFile = new MockMultipartFile(TEST_FILE_NAME, new ClassPathResource(TEST_FILE_NAME).getInputStream());
        List<CsvNameSampleDto> expected = List.of(
                CsvNameSampleDto.builder().no("01").name("john").yearMonth("2023-01").amount(10000.0).build(),
                CsvNameSampleDto.builder().no("01").name("dohun").yearMonth("2023-02").amount(20000.0).build(),
                CsvNameSampleDto.builder().no("01").name("huk").yearMonth("2023-03").amount(0.0).build()
        );

        // when
        BigCsvParser<CsvNameSampleDto> bigCsvParser = new BigCsvParser<>(CsvNameSampleDto.class);
        bigCsvParser.processByChunk(mockMultipartFile, yourCustomConsumer(expected));
    }

    private Consumer<List<CsvNameSampleDto>> yourCustomConsumer(List<CsvNameSampleDto> expected) {
        return (parsedRows -> {
            log.info("parsedRows = " + parsedRows);
            assertThat(parsedRows.size()).isEqualTo(expected.size());
            assertThat(expected).containsExactlyElementsOf(parsedRows);
        });
    }

    @Test
    @DisplayName("csv 컬럼 위치로 파싱하여 원하는 작업을 수행하는 예시")
    void howToUseBigCsvParserByColumnPosition() throws IOException, CsvParserException {
        // given
        MockMultipartFile mockMultipartFile = new MockMultipartFile(TEST_FILE_NAME, new ClassPathResource(TEST_FILE_NAME).getInputStream());
        List<CsvPositionSampleDto> expected = List.of(
                CsvPositionSampleDto.builder().no("01").name("john").yearMonth("2023-01").amount(10000.0).build(),
                CsvPositionSampleDto.builder().no("01").name("dohun").yearMonth("2023-02").amount(20000.0).build(),
                CsvPositionSampleDto.builder().no("01").name("huk").yearMonth("2023-03").amount(0.0).build()
        );

        // when
        BigCsvParser<CsvPositionSampleDto> bigCsvParser = new BigCsvParser<>(CsvPositionSampleDto.class);
        bigCsvParser.processByChunk(mockMultipartFile, yourCustomConsumer2(expected));
    }

    private Consumer<List<CsvPositionSampleDto>> yourCustomConsumer2(List<CsvPositionSampleDto> expected) {
        return (parsedRows -> {
            log.info("parsedRows = " + parsedRows);
            assertThat(parsedRows.size()).isEqualTo(expected.size());
            assertThat(expected).containsExactlyElementsOf(parsedRows);
        });
    }

    @Test
    @DisplayName("파싱 에러 발생하는 경우 몇 번째 행에서 어떤 값에서 발생했는지 알려준다.")
    void tellMeTheCsvRowNumberThatFailedParsing() throws IOException, CsvParserException {
        // given
        MockMultipartFile mockMultipartFile = new MockMultipartFile(EXCEPTION_TEST_FILE_NAME, new ClassPathResource(EXCEPTION_TEST_FILE_NAME).getInputStream());

        // when
        BigCsvParser<CsvNameSampleDto> bigCsvParser = new BigCsvParser<>(CsvNameSampleDto.class, 5);
        CsvParserException exception = Assertions.assertThrows(CsvParserException.class, () -> {
            bigCsvParser.processByChunk(mockMultipartFile, (dtos) -> {
                // 후처리 로직에서 아무것도 수행안함
            });
        });
        // then
        assertThat(exception.getMessage()).isEqualTo("9행 파싱 작업 중 예외 발생");
        assertThat(exception.getCause().getCause().getMessage())
                .isEqualTo("202302XX is invalid format. valid format is e.g. 202101");
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CsvNameSampleDto implements CsvMappingClass {

        @CsvBindByName(column = "NO")
        private String no;

        @CsvBindByName(column = "NAME")
        private String name;

        @CsvCustomBindByName(column = "YYMM", converter = CsvYYYYMMConverter.class)
        private String yearMonth;

        @CsvCustomBindByName(column = "AMT", converter = CsvDoubleConverter.class)
        private Double amount;

    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CsvPositionSampleDto implements CsvMappingClass {

        @CsvBindByPosition(position = 0)
        private String no;

        @CsvBindByPosition(position = 1)
        private String name;

        @CsvCustomBindByPosition(position = 2, converter = CsvYYYYMMConverter.class)
        private String yearMonth;

        @CsvCustomBindByPosition(position = 3, converter = CsvDoubleConverter.class)
        private Double amount;

    }
}
