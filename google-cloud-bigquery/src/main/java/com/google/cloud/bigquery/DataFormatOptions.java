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

  private final boolean useInt64Timestamp;
  private final TimestampFormatOptions timestampFormatOptions;

  DataFormatOptions() {
    this(new Builder());
  }

  DataFormatOptions(Builder builder) {
    this.useInt64Timestamp = builder.useInt64Timestamp;
    this.timestampFormatOptions = builder.timestampFormatOptions;
  }

  public boolean isUseInt64Timestamp() {
    return useInt64Timestamp;
  }

  public TimestampFormatOptions getTimestampFormatOptions() {
    return timestampFormatOptions;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  public static class Builder {
    private boolean useInt64Timestamp;
    private TimestampFormatOptions timestampFormatOptions =
        TimestampFormatOptions.TIMESTAMP_OUTPUT_FORMAT_UNSPECIFIED;

    public Builder() {}

    public Builder(DataFormatOptions dataFormatOptions) {
      this.useInt64Timestamp = dataFormatOptions.useInt64Timestamp;
      this.timestampFormatOptions = dataFormatOptions.timestampFormatOptions;
    }

    public Builder setUseInt64Timestamp(boolean useInt64Timestamp) {
      this.useInt64Timestamp = useInt64Timestamp;
      return this;
    }

    public Builder setTimestampFormatOptions(TimestampFormatOptions timestampFormatOptions) {
      this.timestampFormatOptions = timestampFormatOptions;
      return this;
    }

    public DataFormatOptions build() {
      return new DataFormatOptions(this);
    }
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
    return new Builder()
        .setUseInt64Timestamp(request.getUseInt64Timestamp())
        .setTimestampFormatOptions(
            TimestampFormatOptions.valueOf(request.getTimestampOutputFormat()))
        .build();
  }
}
