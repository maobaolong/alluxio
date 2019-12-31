package alluxio.metrics.sink;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import org.apache.commons.lang.StringUtils;

import java.util.Properties;

/**
 * A regex metrics filter.
 */
public class RegexMetricFilter implements MetricFilter {
  private static final String SLF4J_KEY_FILTER_REGEX = "filter-regex";

  private final Properties mProperties;
  private final String mRegex;

  /**
   * Creates a new {@link RegexMetricFilter} with a {@link Properties}.
   *
   * @param properties the properties which may contain filter-regex properties
   */
  public RegexMetricFilter(Properties properties) {
    mProperties = properties;
    mRegex = getRegex();
  }

  @Override
  public boolean matches(String name, Metric metric) {
    if (mRegex != null) {
      return name.matches(mRegex);
    } else {
      return true;
    }
  }

  /**
   * Gets the regex of filter.
   *
   * @return the regex of filter set by properties, If it is not set or blank, a null value is
   *         returned.
   */
  private String getRegex() {
    String regex = mProperties.getProperty(SLF4J_KEY_FILTER_REGEX);
    if (StringUtils.isBlank(regex)) {
      regex = null;
    }
    return regex;
  }
}
