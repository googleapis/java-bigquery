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

package com.example.bigquery;

// [START bigquery_auth_drive_scope]
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;

public class AuthDriveScope {

  public static void setAuthDriveScope() throws IOException {
    // Create credentials with Drive & BigQuery API scopes.
    // Both APIs must be enabled for your project before running this code.
    String BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery";
    String GOOGLE_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive";
    GoogleCredentials credentials =
        ServiceAccountCredentials.getApplicationDefault()
            .createScoped(ImmutableSet.of(BIGQUERY_SCOPE, GOOGLE_DRIVE_SCOPE));

    // Instantiate a client.
    BigQuery bigquery =
        BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();

    // Use the client.
    System.out.println("Datasets:");
    for (Dataset dataset : bigquery.listDatasets().iterateAll()) {
      System.out.printf(
          "Auth succeeded with multiple scopes. Dataset %s%n", dataset.getDatasetId().getDataset());
    }
  }
}
// [END bigquery_auth_drive_scope]
