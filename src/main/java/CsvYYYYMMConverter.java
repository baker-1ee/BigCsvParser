import com.opencsv.bean.AbstractBeanField;
import com.opencsv.exceptions.CsvDataTypeMismatchException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CsvYYYYMMConverter extends AbstractBeanField {
    @Override
    protected Object convert(String value) throws CsvDataTypeMismatchException {
        if (!isValidFormat(value)) {
            throw new CsvDataTypeMismatchException(String.format("%s is invalid format. valid format is e.g. 202101", value));
        }
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMM");
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM");
        try {
            Date date = inputFormat.parse(value);
            return outputFormat.format(date);
        } catch (ParseException e) {
            throw new CsvDataTypeMismatchException(String.format("parsing exception occur about %s", value));
        }
    }

    private boolean isValidFormat(String value) {
        // value가 "yyyyMM" 형식인지 체크
        return value.matches("^(19|20)\\d{2}(0[1-9]|1[0-2])$");
    }
}
