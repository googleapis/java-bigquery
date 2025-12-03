package com.google.cloud.bigquery;

/**
 * Google BigQuery DataFormatOptions. Configures the output format for data types returned from
 * BigQuery.
 */
public class DataFormatOptions {
  public enum TimestampFormatOptions {
    TIMESTAMP_OUTPUT_FORMAT_UNSPECIFIED("TIMESTAMP_OUTPUT_FORMAT_UNSPECIFIED"),
    FLOAT64("FLOAT64"),
    INT64("INT64"),
    ISO8601_STRING("ISO8601_STRING");

    private final String format;

    TimestampFormatOptions(String format) {
      this.format = format;
    }

    @Override
    public String toString() {
      return format;
    }
  }

  private boolean useInt64Timestamp;
  private TimestampFormatOptions timestampFormatOptions;

  public DataFormatOptions(
      boolean useInt64Timestamp, TimestampFormatOptions timestampFormatOptions) {
    this.useInt64Timestamp = useInt64Timestamp;
    this.timestampFormatOptions = timestampFormatOptions;
  }

  public boolean isUseInt64Timestamp() {
    return useInt64Timestamp;
  }

  public void setUseInt64Timestamp(boolean useInt64Timestamp) {
    this.useInt64Timestamp = useInt64Timestamp;
  }

  public TimestampFormatOptions getTimestampFormatOptions() {
    return timestampFormatOptions;
  }

  public void setTimestampFormatOptions(TimestampFormatOptions timestampFormatOptions) {
    this.timestampFormatOptions = timestampFormatOptions;
  }

  com.google.api.services.bigquery.model.DataFormatOptions toPb() {
    com.google.api.services.bigquery.model.DataFormatOptions request =
        new com.google.api.services.bigquery.model.DataFormatOptions();
    request.setUseInt64Timestamp(useInt64Timestamp);
    if (timestampFormatOptions != null) {
      request.setTimestampOutputFormat(timestampFormatOptions.toString());
    }
    return request;
  }

  DataFormatOptions fromPb(com.google.api.services.bigquery.model.DataFormatOptions request) {
    return new DataFormatOptions(
        request.getUseInt64Timestamp(),
        TimestampFormatOptions.valueOf(request.getTimestampOutputFormat()));
  }
}
