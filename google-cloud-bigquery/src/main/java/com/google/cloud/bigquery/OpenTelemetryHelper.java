package com.google.cloud.bigquery;

import com.google.api.core.InternalApi;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.cloud.Policy;
import com.google.cloud.bigquery.BigQuery.IAMOption;
import com.google.cloud.bigquery.spi.v2.BigQueryRpc;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@InternalApi
public final class OpenTelemetryHelper {
  private final boolean enabled;
  private final Tracer tracer;

  private String getFieldAsString(Object field) {
    return field == null ? "null" : field.toString();
  }

  OpenTelemetryHelper(BigQueryOptions options) {
    enabled = options.isOpenTelemetryTracingEnabled();
    tracer = options.getOpenTelemetryTracer();
  }
  public Span datasetCreateSpan(DatasetInfo datasetInfo, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("datasetCreate")
        .setAttribute("datasetId", datasetInfo.getDatasetId().getDataset())
        .setAttribute("defaultTableLifetime", getFieldAsString(datasetInfo.getDefaultTableLifetime()))
        .setAttribute("description", getFieldAsString(datasetInfo.getDescription()))
        .setAttribute("etag", getFieldAsString(datasetInfo.getEtag()))
        .setAttribute("friendlyName", getFieldAsString(datasetInfo.getFriendlyName()))
        .setAttribute("generatedId", getFieldAsString(datasetInfo.getGeneratedId()))
        .setAttribute("lastModified", getFieldAsString(datasetInfo.getLastModified()))
        .setAttribute("location", getFieldAsString(datasetInfo.getLocation()))
        .setAttribute("selfLink", getFieldAsString(datasetInfo.getSelfLink()))
        .setAttribute("labels", getFieldAsString(datasetInfo.getLabels()))
        .setAttribute("defaultEncryptionConfiguration", getFieldAsString(datasetInfo.getDefaultEncryptionConfiguration()))
        .setAttribute("defaultPartitionExpirationMs", getFieldAsString(datasetInfo.getDefaultPartitionExpirationMs()))
        .setAttribute("defaultCollation", getFieldAsString(datasetInfo.getDefaultCollation()))
        .setAttribute("externalDatasetReference", getFieldAsString(datasetInfo.getExternalDatasetReference()))
        .setAttribute("storageBillingModel", getFieldAsString(datasetInfo.getStorageBillingModel()))
        .setAttribute("maxTimeTravelHours", getFieldAsString(datasetInfo.getMaxTimeTravelHours()))
        .setAttribute("resourceTags", getFieldAsString(datasetInfo.getResourceTags()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span datasetGetSpan(DatasetId datasetId, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("datasetGet")
        .setAttribute("datasetId", datasetId.getDataset())
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span datasetsListSpan(String projectId, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("datasetList")
        .setAttribute("projectId", projectId)
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span datasetUpdateSpan(DatasetInfo datasetInfo, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("datasetUpdate")
        .setAttribute("datasetId", datasetInfo.getDatasetId().getDataset())
        .setAttribute("defaultTableLifetime", getFieldAsString(datasetInfo.getDefaultTableLifetime()))
        .setAttribute("description", getFieldAsString(datasetInfo.getDescription()))
        .setAttribute("etag", getFieldAsString(datasetInfo.getEtag()))
        .setAttribute("friendlyName", getFieldAsString(datasetInfo.getFriendlyName()))
        .setAttribute("generatedId", getFieldAsString(datasetInfo.getGeneratedId()))
        .setAttribute("lastModified", getFieldAsString(datasetInfo.getLastModified()))
        .setAttribute("location", getFieldAsString(datasetInfo.getLocation()))
        .setAttribute("selfLink", getFieldAsString(datasetInfo.getSelfLink()))
        .setAttribute("labels", getFieldAsString(datasetInfo.getLabels()))
        .setAttribute("defaultEncryptionConfiguration", getFieldAsString(datasetInfo.getDefaultEncryptionConfiguration()))
        .setAttribute("defaultPartitionExpirationMs", getFieldAsString(datasetInfo.getDefaultPartitionExpirationMs()))
        .setAttribute("defaultCollation", getFieldAsString(datasetInfo.getDefaultCollation()))
        .setAttribute("externalDatasetReference", getFieldAsString(datasetInfo.getExternalDatasetReference()))
        .setAttribute("storageBillingModel", getFieldAsString(datasetInfo.getStorageBillingModel()))
        .setAttribute("maxTimeTravelHours", getFieldAsString(datasetInfo.getMaxTimeTravelHours()))
        .setAttribute("resourceTags", getFieldAsString(datasetInfo.getResourceTags()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span datasetDeleteSpan(DatasetId datasetId, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("datasetDelete")
        .setAttribute("datasetId", datasetId.getDataset())
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span tableCreateSpan(TableInfo tableInfo, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("tableCreate")
        .setAttribute("etag", getFieldAsString(tableInfo.getEtag()))
        .setAttribute("generatedId", getFieldAsString(tableInfo.getGeneratedId()))
        .setAttribute("selfLink", getFieldAsString(tableInfo.getSelfLink()))
        .setAttribute("tableId", tableInfo.getTableId().getTable())
        .setAttribute("friendlyName", getFieldAsString(tableInfo.getFriendlyName()))
        .setAttribute("description", getFieldAsString(tableInfo.getDescription()))
        .setAttribute("creationTime", getFieldAsString(tableInfo.getCreationTime()))
        .setAttribute("expirationTime", getFieldAsString(tableInfo.getExpirationTime()))
        .setAttribute("lastModifiedTime", getFieldAsString(tableInfo.getLastModifiedTime()))
        .setAttribute("numBytes", getFieldAsString(tableInfo.getNumBytes()))
        .setAttribute("numLongTermBytes", getFieldAsString(tableInfo.getNumLongTermBytes()))
        .setAttribute("numTimeTravelPhysicalBytes", getFieldAsString(tableInfo.getNumTimeTravelPhysicalBytes()))
        .setAttribute("numTotalLogicalBytes", getFieldAsString(tableInfo.getNumTotalLogicalBytes()))
        .setAttribute("numActiveLogicalBytes", getFieldAsString(tableInfo.getNumActiveLogicalBytes()))
        .setAttribute("numLongTermLogicalBytes", getFieldAsString(tableInfo.getNumLongTermLogicalBytes()))
        .setAttribute("numTotalPhysicalBytes", getFieldAsString(tableInfo.getNumTotalPhysicalBytes()))
        .setAttribute("numActivePhysicalBytes", getFieldAsString(tableInfo.getNumActivePhysicalBytes()))
        .setAttribute("numLongTermPhysicalBytes", getFieldAsString(tableInfo.getNumLongTermPhysicalBytes()))
        .setAttribute("numRows", getFieldAsString(tableInfo.getNumRows()))
        .setAttribute("definition", getFieldAsString(tableInfo.getDefinition()))
        .setAttribute("encryptionConfiguration", getFieldAsString(tableInfo.getEncryptionConfiguration()))
        .setAttribute("labels", getFieldAsString(tableInfo.getLabels()))
        .setAttribute("resourceTags", getFieldAsString(tableInfo.getResourceTags()))
        .setAttribute("requirePartitionFilter", getFieldAsString(tableInfo.getRequirePartitionFilter()))
        .setAttribute("defaultCollation", getFieldAsString(tableInfo.getDefaultCollation()))
        .setAttribute("cloneDefinition", getFieldAsString(tableInfo.getCloneDefinition()))
        .setAttribute("tableConstraints", getFieldAsString(tableInfo.getTableConstraints()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span tableGetSpan(TableId tableId, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("tableGet")
        .setAttribute("tableId", tableId.getTable())
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span tablesListSpan(DatasetId datasetId, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("tablesList")
        .setAttribute("datasetId", datasetId.getDataset())
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span tableDataListSpan(TableId tableId, Schema schema, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("tableDataList")
        .setAttribute("tableId", tableId.getTable())
        .setAttribute("schema", getFieldAsString(schema))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span tableUpdateSpan(TableInfo tableInfo, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("tableUpdate")
        .setAttribute("etag", getFieldAsString(tableInfo.getEtag()))
        .setAttribute("generatedId", getFieldAsString(tableInfo.getGeneratedId()))
        .setAttribute("selfLink", getFieldAsString(tableInfo.getSelfLink()))
        .setAttribute("tableId", tableInfo.getTableId().getTable())
        .setAttribute("friendlyName", getFieldAsString(tableInfo.getFriendlyName()))
        .setAttribute("description", getFieldAsString(tableInfo.getDescription()))
        .setAttribute("creationTime", getFieldAsString(tableInfo.getCreationTime()))
        .setAttribute("expirationTime", getFieldAsString(tableInfo.getExpirationTime()))
        .setAttribute("lastModifiedTime", getFieldAsString(tableInfo.getLastModifiedTime()))
        .setAttribute("numBytes", getFieldAsString(tableInfo.getNumBytes()))
        .setAttribute("numLongTermBytes", getFieldAsString(tableInfo.getNumLongTermBytes()))
        .setAttribute("numTimeTravelPhysicalBytes", getFieldAsString(tableInfo.getNumTimeTravelPhysicalBytes()))
        .setAttribute("numTotalLogicalBytes", getFieldAsString(tableInfo.getNumTotalLogicalBytes()))
        .setAttribute("numActiveLogicalBytes", getFieldAsString(tableInfo.getNumActiveLogicalBytes()))
        .setAttribute("numLongTermLogicalBytes", getFieldAsString(tableInfo.getNumLongTermLogicalBytes()))
        .setAttribute("numTotalPhysicalBytes", getFieldAsString(tableInfo.getNumTotalPhysicalBytes()))
        .setAttribute("numActivePhysicalBytes", getFieldAsString(tableInfo.getNumActivePhysicalBytes()))
        .setAttribute("numLongTermPhysicalBytes", getFieldAsString(tableInfo.getNumLongTermPhysicalBytes()))
        .setAttribute("numRows", getFieldAsString(tableInfo.getNumRows()))
        .setAttribute("definition", getFieldAsString(tableInfo.getDefinition()))
        .setAttribute("encryptionConfiguration", getFieldAsString(tableInfo.getEncryptionConfiguration()))
        .setAttribute("labels", getFieldAsString(tableInfo.getLabels()))
        .setAttribute("resourceTags", getFieldAsString(tableInfo.getResourceTags()))
        .setAttribute("requirePartitionFilter", getFieldAsString(tableInfo.getRequirePartitionFilter()))
        .setAttribute("defaultCollation", getFieldAsString(tableInfo.getDefaultCollation()))
        .setAttribute("cloneDefinition", getFieldAsString(tableInfo.getCloneDefinition()))
        .setAttribute("tableConstraints", getFieldAsString(tableInfo.getTableConstraints()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span tableDeleteSpan(TableId tableId) {
    if (!enabled) {
      return null;
    }
    return tracer.spanBuilder("tableDelete")
        .setAttribute("tableId", tableId.getTable())
        .startSpan();
  }

  public Span routineCreateSpan(RoutineInfo routineInfo, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("routineCreate")
        .setAttribute("routineId", getFieldAsString(routineInfo.getRoutineId().getRoutine()))
        .setAttribute("etag", getFieldAsString(routineInfo.getEtag()))
        .setAttribute("routineType", getFieldAsString(routineInfo.getRoutineType()))
        .setAttribute("creationTime", getFieldAsString(routineInfo.getCreationTime()))
        .setAttribute("description", getFieldAsString(routineInfo.getDescription()))
        .setAttribute("determinismLevel", getFieldAsString(routineInfo.getDeterminismLevel()))
        .setAttribute("lastModifiedTime", getFieldAsString(routineInfo.getLastModifiedTime()))
        .setAttribute("language", getFieldAsString(routineInfo.getLanguage()))
        .setAttribute("argumentList", getFieldAsString(routineInfo.getArguments()))
        .setAttribute("returnType", getFieldAsString(routineInfo.getReturnType()))
        .setAttribute("returnableType", getFieldAsString(routineInfo.getReturnTableType()))
        .setAttribute("importedLibrariesList", getFieldAsString(routineInfo.getImportedLibraries()))
        .setAttribute("body", getFieldAsString(routineInfo.getBody()))
        .setAttribute("remoteFunctionOptions", getFieldAsString(routineInfo.getRemoteFunctionOptions()))
        .setAttribute("dataGovernanceTyoe", getFieldAsString(routineInfo.getDataGovernanceType()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span routineGetSpan(RoutineId routineId, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("routineGet")
        .setAttribute("routine", getFieldAsString(routineId.getRoutine()))
        .setAttribute("dataset", getFieldAsString(routineId.getDataset()))
        .setAttribute("project", getFieldAsString(routineId.getProject()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span routinesListSpan(DatasetId datasetId, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("routinesList")
        .setAttribute("datasetId", datasetId.getDataset())
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span routineUpdateSpan(RoutineInfo routineInfo, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("routineUpdate")
        .setAttribute("routineId", getFieldAsString(routineInfo.getRoutineId().getRoutine()))
        .setAttribute("etag", getFieldAsString(routineInfo.getEtag()))
        .setAttribute("routineType", getFieldAsString(routineInfo.getRoutineType()))
        .setAttribute("creationTime", getFieldAsString(routineInfo.getCreationTime()))
        .setAttribute("description", getFieldAsString(routineInfo.getDescription()))
        .setAttribute("determinismLevel", getFieldAsString(routineInfo.getDeterminismLevel()))
        .setAttribute("lastModifiedTime", getFieldAsString(routineInfo.getLastModifiedTime()))
        .setAttribute("language", getFieldAsString(routineInfo.getLanguage()))
        .setAttribute("argumentList", getFieldAsString(routineInfo.getArguments()))
        .setAttribute("returnType", getFieldAsString(routineInfo.getReturnType()))
        .setAttribute("returnableType", getFieldAsString(routineInfo.getReturnTableType()))
        .setAttribute("importedLibrariesList", getFieldAsString(routineInfo.getImportedLibraries()))
        .setAttribute("body", getFieldAsString(routineInfo.getBody()))
        .setAttribute("remoteFunctionOptions", getFieldAsString(routineInfo.getRemoteFunctionOptions()))
        .setAttribute("dataGovernanceTyoe", getFieldAsString(routineInfo.getDataGovernanceType()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span routineDeleteSpan(RoutineId routineId) {
    if (!enabled) {
      return null;
    }
    return tracer.spanBuilder("routineDelete")
        .setAttribute("routine", getFieldAsString(routineId.getRoutine()))
        .setAttribute("dataset", getFieldAsString(routineId.getDataset()))
        .setAttribute("project", getFieldAsString(routineId.getProject()))
        .startSpan();
  }

  public Span modelGetSpan(ModelId modelId, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("modelGet")
        .setAttribute("model", getFieldAsString(modelId.getModel()))
        .setAttribute("dataset", getFieldAsString(modelId.getDataset()))
        .setAttribute("project", getFieldAsString(modelId.getProject()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span modelsListSpan(DatasetId datasetId, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("modelsList")
        .setAttribute("datasetId", datasetId.getDataset())
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span modelUpdateSpan(ModelInfo modelInfo, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("modelUpdate")
        .setAttribute("etag", getFieldAsString(modelInfo.getEtag()))
        .setAttribute("modelId", getFieldAsString(modelInfo.getModelId().getModel()))
        .setAttribute("description", getFieldAsString(modelInfo.getDescription()))
        .setAttribute("modelType", getFieldAsString(modelInfo.getModelType()))
        .setAttribute("friendlyName", getFieldAsString(modelInfo.getFriendlyName()))
        .setAttribute("creationTime", getFieldAsString(modelInfo.getCreationTime()))
        .setAttribute("lastModifiedTime", getFieldAsString(modelInfo.getLastModifiedTime()))
        .setAttribute("expirationTime", getFieldAsString(modelInfo.getExpirationTime()))
        .setAttribute("labels", getFieldAsString(modelInfo.getLabels()))
        .setAttribute("location", getFieldAsString(modelInfo.getLocation()))
        .setAttribute("trainingRunList", getFieldAsString(modelInfo.getTrainingRuns()))
        .setAttribute("featureColumnList", getFieldAsString(modelInfo.getFeatureColumns()))
        .setAttribute("labelColumnList", getFieldAsString(modelInfo.getLabelColumns()))
        .setAttribute("encryptionConfiguration", getFieldAsString(modelInfo.getEncryptionConfiguration()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span modelDeleteSpan(ModelId modelId) {
    if (!enabled) {
      return null;
    }
    return tracer.spanBuilder("modelDelete")
        .setAttribute("model", getFieldAsString(modelId.getModel()))
        .setAttribute("dataset", getFieldAsString(modelId.getDataset()))
        .setAttribute("project", getFieldAsString(modelId.getProject()))
        .startSpan();
  }

  public Span jobCreateSpan(JobInfo jobInfo, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("jobCreate")
        .setAttribute("etag", getFieldAsString(jobInfo.getEtag()))
        .setAttribute("generatedId", getFieldAsString(jobInfo.getGeneratedId()))
        .setAttribute("jobId", getFieldAsString(jobInfo.getJobId().getJob()))
        .setAttribute("selfLink", getFieldAsString(jobInfo.getSelfLink()))
        .setAttribute("status", getFieldAsString(jobInfo.getStatus()))
        .setAttribute("statistics", getFieldAsString(jobInfo.getStatistics()))
        .setAttribute("userEmail", getFieldAsString(jobInfo.getUserEmail()))
        .setAttribute("configuration", getFieldAsString(jobInfo.getConfiguration()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span jobGetSpan(JobId jobId, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("jobGet")
        .setAttribute("job", getFieldAsString(jobId.getJob()))
        .setAttribute("location", getFieldAsString(jobId.getLocation()))
        .setAttribute("project", getFieldAsString(jobId.getProject()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span jobsListSpan(Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("jobsList").startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span jobCancelSpan(JobId jobId) {
    if (!enabled) {
      return null;
    }
    return tracer.spanBuilder("jobCancel")
        .setAttribute("job", getFieldAsString(jobId.getJob()))
        .setAttribute("location", getFieldAsString(jobId.getLocation()))
        .setAttribute("project", getFieldAsString(jobId.getProject()))
        .startSpan();
  }

  public Span insertAllSpan(InsertAllRequest request) {
    if (!enabled) {
      return null;
    }
    return tracer.spanBuilder("insertAll")
        .setAttribute("table", getFieldAsString(request.getTable().getTable()))
        .setAttribute("row", getFieldAsString(request.getRows()))
        .setAttribute("templateSuffix", getFieldAsString(request.getTemplateSuffix()))
        .setAttribute("ignoreUnknownValues", getFieldAsString(request.ignoreUnknownValues()))
        .setAttribute("skipInvalidRows", getFieldAsString(request.skipInvalidRows()))
        .startSpan();
  }

  public Span iamPolicyGet(TableId tableId, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("iamPolicyGet")
        .setAttribute("tableId", tableId.getTable())
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span iamPolicySet(TableId tableId, Policy policy, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("iamPolicySet")
        .setAttribute("tableId", tableId.getTable())
        .setAttribute("version", getFieldAsString(policy.getVersion()))
        .setAttribute("bindings", getFieldAsString(policy.getBindings()))
        .setAttribute("bindingsList", getFieldAsString(policy.getBindingsList()))
        .setAttribute("etag", getFieldAsString(policy.getEtag()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span testIamPermissions(TableId tableId, List<String> permissions, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("testIamPermissions")
        .setAttribute("tableId", tableId.getTable())
        .setAttribute("permissions", getFieldAsString(permissions))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span queryRpc(String projectId, QueryRequest content, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("queryRpc")
        .setAttribute("projectId", projectId)
        .setAttribute("continuous", getFieldAsString(content.getContinuous()))
        .setAttribute("createSession", getFieldAsString(content.getCreateSession()))
        .setAttribute("defaultDataset", getFieldAsString(content.getDefaultDataset()))
        .setAttribute("destinationEncryptionConfiguration", getFieldAsString(content.getDestinationEncryptionConfiguration()))
        .setAttribute("dryRun", getFieldAsString(content.getDryRun()))
        .setAttribute("formatOptions", getFieldAsString(content.getFormatOptions()))
        .setAttribute("jobCreationMode", getFieldAsString(content.getJobCreationMode()))
        .setAttribute("jobTimeoutMs", getFieldAsString(content.getJobTimeoutMs()))
        .setAttribute("kind", getFieldAsString(content.getKind()))
        .setAttribute("labels", getFieldAsString(content.getLabels()))
        .setAttribute("location", getFieldAsString(content.getLocation()))
        .setAttribute("maxResults", getFieldAsString(content.getMaxResults()))
        .setAttribute("maximumBytesBilled", getFieldAsString(content.getMaximumBytesBilled()))
        .setAttribute("parameterMode", getFieldAsString(content.getParameterMode()))
        .setAttribute("preserveNulls", getFieldAsString(content.getPreserveNulls()))
        .setAttribute("query", getFieldAsString(content.getQuery()))
        .setAttribute("queryParameters", getFieldAsString(content.getQueryParameters()))
        .setAttribute("requestId", getFieldAsString(content.getRequestId()))
        .setAttribute("reservation", getFieldAsString(content.getReservation()))
        .setAttribute("timeoutMs", getFieldAsString(content.getTimeoutMs()))
        .setAttribute("useLegacySql", getFieldAsString(content.getUseLegacySql()))
        .setAttribute("useQueryCache", getFieldAsString(content.getUseQueryCache()))
        .setAttribute("writeIncrementalResults", getFieldAsString(content.getWriteIncrementalResults()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }

  public Span getQueryResults(JobId jobId, Map<BigQueryRpc.Option, ?> optionsMap) {
    if (!enabled) {
      return null;
    }
    Span span = tracer.spanBuilder("getQueryResults")
        .setAttribute("job", getFieldAsString(jobId.getJob()))
        .setAttribute("location", getFieldAsString(jobId.getLocation()))
        .setAttribute("project", getFieldAsString(jobId.getProject()))
        .startSpan();

    for (Entry<BigQueryRpc.Option, ?> entry : optionsMap.entrySet()) {
      span.setAttribute(entry.getKey().toString(), entry.getValue().toString());
    }
    return span;
  }
}
