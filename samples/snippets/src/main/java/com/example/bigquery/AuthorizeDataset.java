/*
 * Copyright 2022 Google LLC
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

// [START bigquery_authorize_dataset_scope]
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;

public class AuthorizeDataset {

    public static void main(String[] args) {
        // TODO(developer): Replace these variables before running the sample.
        String projectId = "PROJECT_ID";
        String sharedDatasetName = "SHARED_DATASET_NAME";
        String authorizedDatasetName = "AUTHORIZED_DATASET_NAME";
        authorizeDataset(projectId, sharedDatasetName, authorizedDatasetName);
    }

    public static void authorizeDataset(
            String projectId, String sharedDatasetName, String authorizedDatasetName) {

        try {
            DatasetId sharedDatasetId = DatasetId.of(projectId, sharedDatasetName);

            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            List<String> targetTypes = ImmutableList.of("VIEWS");
            // Specify the acl which will be shared to the authorized dataset
            List<Acl> acl =
                    ImmutableList.of(
                            Acl.of(new Acl.Group("projectOwners"), Acl.Role.OWNER),
                            Acl.of(new Acl.IamMember("allUsers"), Acl.Role.READER));
            DatasetInfo datasetInfo =
                    DatasetInfo.newBuilder(sharedDatasetId)
                            .setAcl(acl)
                            .setDescription("shared Dataset")
                            .build();

            // create shared dataset
            Dataset sharedDataset = bigquery.create(datasetInfo);
            System.out.printf("Created sharedDataset: %s\n", sharedDataset.getDatasetId());

            // Get the current metadata for the dataset you want to share by calling the datasets.getAcl
            // method
            List<Acl> sharedDatasetAcl = new ArrayList<>(sharedDataset.getAcl());

            // Create a new dataset to be authorized
            DatasetId authorizedDatasetId = DatasetId.of(projectId, authorizedDatasetName);
            DatasetInfo authorizedDatasetInfo =
                    DatasetInfo.newBuilder(authorizedDatasetId)
                            .setDescription("new Dataset to be authorized by the sharedDataset")
                            .build();
            Dataset authorizedDataset = bigquery.create(authorizedDatasetInfo);
            System.out.printf("Created authorizedDataset: %s\n", authorizedDataset.getDatasetId());

            // Add the new DatasetAccessEntry object to the existing sharedDatasetAcl list
            Acl.DatasetAclEntity datasetEntity =
                    new Acl.DatasetAclEntity(authorizedDatasetId, targetTypes);
            sharedDatasetAcl.add(Acl.of(datasetEntity));
            // Update the dataset with the added authorization
            Dataset updatedDataset = sharedDataset.toBuilder().setAcl(sharedDatasetAcl).build().update();

            System.out.printf(
                    "Dataset %s updated with the added authorization\n", updatedDataset.getDatasetId());

        } catch (BigQueryException e) {
            System.out.println("Dataset Authorization failed due to error: \n" + e);
        }
    }
}
// [END bigquery_authorize_dataset_scope]
