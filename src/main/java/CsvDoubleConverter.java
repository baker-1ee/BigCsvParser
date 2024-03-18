import com.opencsv.bean.AbstractBeanField;
import com.opencsv.exceptions.CsvDataTypeMismatchException;

public class CsvDoubleConverter extends AbstractBeanField {
    @Override
    protected Double convert(String value) throws CsvDataTypeMismatchException {
        try {
            return value.isEmpty() ? 0d : Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new CsvDataTypeMismatchException(String.format("%s is not a valid number.", value));
        }
    }
}
