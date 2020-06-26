/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigquery;

import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.common.collect.Lists;

final class QueryRequestInfo {

  private QueryJobConfiguration config;

  QueryRequestInfo(QueryJobConfiguration config) {
    this.config = config;
  }

  boolean isFastQuerySupported() {
    return config.getClustering() == null
        && config.getCreateDisposition() == null
        && config.getDestinationEncryptionConfiguration() == null
        && config.getDestinationTable() == null
        && config.getMaximumBillingTier() == null
        && config.getPriority() == null
        && config.getRangePartitioning() == null
        && config.getSchemaUpdateOptions() == null
        && config.getTableDefinitions() == null
        && config.getTimePartitioning() == null
        && config.getUserDefinedFunctions() == null
        && config.getWriteDisposition() == null;
  }

  QueryRequest toPb() {
    QueryRequest query = new QueryRequest();
    if (config.getConnectionProperties() != null) {
      query.setConnectionProperties(
          Lists.transform(config.getConnectionProperties(), ConnectionProperty.TO_PB_FUNCTION));
    }
    if (config.getDefaultDataset() != null) {
      query.setDefaultDataset(config.getDefaultDataset().toPb());
    }
    if (config.dryRun() != null) {
      query.setDryRun(config.dryRun());
    }
    if (config.getLabels() != null) {
      query.setLabels(config.getLabels());
    }
    if (config.getMaximumBytesBilled() != null) {
      query.setMaximumBytesBilled(config.getMaximumBytesBilled());
    }
    query.setQuery(config.getQuery());
    // TODO: add back when supported
    // query.setRequestId(UUID.randomUUID().toString());
    JobConfiguration jobConfiguration = config.toPb();
    JobConfigurationQuery configurationQuery = jobConfiguration.getQuery();
    if (configurationQuery.getQueryParameters() != null) {
      query.setQueryParameters(configurationQuery.getQueryParameters());
    }
    if (config.useLegacySql() != null) {
      query.setUseLegacySql(config.useLegacySql());
    }
    if (config.useQueryCache() != null) {
      query.setUseQueryCache(config.useQueryCache());
    }
    return query;
  }
}
