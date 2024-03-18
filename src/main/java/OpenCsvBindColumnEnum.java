import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvBindByPosition;
import com.opencsv.bean.CsvCustomBindByName;
import com.opencsv.bean.CsvCustomBindByPosition;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;

@RequiredArgsConstructor
@Getter
public enum OpenCsvBindColumnEnum {
    COLUMN_NAME(0, List.of(CsvBindByName.class, CsvCustomBindByName.class)),
    COLUMN_POSITION(1, List.of(CsvBindByPosition.class, CsvCustomBindByPosition.class));

    private final int skipLine;
    private final List<Class<? extends Annotation>> bindAnnotations;

    public static int getSkipLine(Class<? extends CsvMappingClass> tClass) {
        return Arrays.stream(values())
                .filter(openCsvBindColumnEnum -> Arrays.stream(tClass.getDeclaredFields())
                        .anyMatch(field -> openCsvBindColumnEnum.getBindAnnotations().stream().anyMatch(field::isAnnotationPresent)))
                .map(OpenCsvBindColumnEnum::getSkipLine)
                .findFirst()
                .orElse(0);
    }

}
