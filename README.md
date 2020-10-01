# Google Cloud BigQuery Client for Java

Java idiomatic client for [Cloud BigQuery][product-docs].

[![Maven][maven-version-image]][maven-version-link]
![Stability][stability-image]

- [Product Documentation][product-docs]
- [Client Library Documentation][javadocs]

## Quickstart

If you are using Maven with [BOM][libraries-bom], add this to your pom.xml file
```xml
<!--  Using libraries-bom to manage versions.
See https://github.com/GoogleCloudPlatform/cloud-opensource-java/wiki/The-Google-Cloud-Platform-Libraries-BOM -->
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.google.cloud</groupId>
      <artifactId>libraries-bom</artifactId>
      <version>10.0.0</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>com.google.cloud</groupId>
    <artifactId>google-cloud-bigquery</artifactId>
  </dependency>

```

If you are using Maven without BOM, add this to your dependencies:

```xml
<dependency>
  <groupId>com.google.cloud</groupId>
  <artifactId>google-cloud-bigquery</artifactId>
  <version>1.117.1</version>
</dependency>

```

[//]: # ({x-version-update-start:google-cloud-bigquery:released})

If you are using Gradle, add this to your dependencies
```Groovy
compile 'com.google.cloud:google-cloud-bigquery:1.121.0'
```
If you are using SBT, add this to your dependencies
```Scala
libraryDependencies += "com.google.cloud" % "google-cloud-bigquery" % "1.121.0"
```
[//]: # ({x-version-update-end})

## Authentication

See the [Authentication][authentication] section in the base directory's README.

## Getting Started

### Prerequisites

You will need a [Google Cloud Platform Console][developer-console] project with the Cloud BigQuery [API enabled][enable-api].
You will need to [enable billing][enable-billing] to use Google Cloud BigQuery.
[Follow these instructions][create-project] to get your project set up. You will also need to set up the local development environment by
[installing the Google Cloud SDK][cloud-sdk] and running the following commands in command line:
`gcloud auth login` and `gcloud config set project [YOUR PROJECT ID]`.

### Installation and setup

You'll need to obtain the `google-cloud-bigquery` library.  See the [Quickstart](#quickstart) section
to add `google-cloud-bigquery` as a dependency in your code.

## About Cloud BigQuery


[Cloud BigQuery][product-docs] is a fully managed, NoOps, low cost data analytics service.
Data can be streamed into BigQuery at millions of rows per second to enable real-time analysis.
With BigQuery you can easily deploy Petabyte-scale Databases.

See the [Cloud BigQuery client library docs][javadocs] to learn how to
use this Cloud BigQuery Client Library.





## Samples

Samples are in the [`samples/`](https://github.com/googleapis/java-bigquery/tree/master/samples) directory. The samples' `README.md`
has instructions for running the samples.

| Sample                      | Source Code                       | Try it |
| --------------------------- | --------------------------------- | ------ |
| Add Column Load Append | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/AddColumnLoadAppend.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/AddColumnLoadAppend.java) |
| Add Empty Column | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/AddEmptyColumn.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/AddEmptyColumn.java) |
| Auth Drive Scope | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/AuthDriveScope.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/AuthDriveScope.java) |
| Auth Snippets | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/AuthSnippets.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/AuthSnippets.java) |
| Auth User Flow | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/AuthUserFlow.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/AuthUserFlow.java) |
| Auth User Query | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/AuthUserQuery.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/AuthUserQuery.java) |
| Authorized View Tutorial | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/AuthorizedViewTutorial.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/AuthorizedViewTutorial.java) |
| Browse Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/BrowseTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/BrowseTable.java) |
| Cancel Job | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CancelJob.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CancelJob.java) |
| Copy Multiple Tables | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CopyMultipleTables.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CopyMultipleTables.java) |
| Copy Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CopyTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CopyTable.java) |
| Copy Table Cmek | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CopyTableCmek.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CopyTableCmek.java) |
| Create Clustered Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CreateClusteredTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CreateClusteredTable.java) |
| Create Dataset | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CreateDataset.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CreateDataset.java) |
| Create Job | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CreateJob.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CreateJob.java) |
| Create Model | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CreateModel.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CreateModel.java) |
| Create Partitioned Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CreatePartitionedTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CreatePartitionedTable.java) |
| Create Range Partitioned Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CreateRangePartitionedTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CreateRangePartitionedTable.java) |
| Create Routine | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CreateRoutine.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CreateRoutine.java) |
| Create Routine Ddl | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CreateRoutineDdl.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CreateRoutineDdl.java) |
| Create Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CreateTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CreateTable.java) |
| Create Table Cmek | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CreateTableCmek.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CreateTableCmek.java) |
| Create Table Without Schema | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CreateTableWithoutSchema.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CreateTableWithoutSchema.java) |
| Create View | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/CreateView.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/CreateView.java) |
| Dataset Exists | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/DatasetExists.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/DatasetExists.java) |
| Ddl Create View | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/DdlCreateView.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/DdlCreateView.java) |
| Delete Dataset | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/DeleteDataset.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/DeleteDataset.java) |
| Delete Dataset And Contents | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/DeleteDatasetAndContents.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/DeleteDatasetAndContents.java) |
| Delete Label Dataset | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/DeleteLabelDataset.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/DeleteLabelDataset.java) |
| Delete Label Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/DeleteLabelTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/DeleteLabelTable.java) |
| Delete Model | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/DeleteModel.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/DeleteModel.java) |
| Delete Routine | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/DeleteRoutine.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/DeleteRoutine.java) |
| Delete Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/DeleteTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/DeleteTable.java) |
| Extract Table Compressed | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/ExtractTableCompressed.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/ExtractTableCompressed.java) |
| Extract Table To Csv | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/ExtractTableToCsv.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/ExtractTableToCsv.java) |
| Extract Table To Json | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/ExtractTableToJson.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/ExtractTableToJson.java) |
| Get Dataset Info | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/GetDatasetInfo.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/GetDatasetInfo.java) |
| Get Dataset Labels | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/GetDatasetLabels.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/GetDatasetLabels.java) |
| Get Job | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/GetJob.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/GetJob.java) |
| Get Model | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/GetModel.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/GetModel.java) |
| Get Routine | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/GetRoutine.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/GetRoutine.java) |
| Get Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/GetTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/GetTable.java) |
| Get Table Labels | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/GetTableLabels.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/GetTableLabels.java) |
| Get View | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/GetView.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/GetView.java) |
| Grant View Access | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/GrantViewAccess.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/GrantViewAccess.java) |
| Inserting Data Types | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/InsertingDataTypes.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/InsertingDataTypes.java) |
| Label Dataset | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LabelDataset.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LabelDataset.java) |
| Label Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LabelTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LabelTable.java) |
| List Datasets | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/ListDatasets.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/ListDatasets.java) |
| List Datasets By Label | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/ListDatasetsByLabel.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/ListDatasetsByLabel.java) |
| List Jobs | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/ListJobs.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/ListJobs.java) |
| List Models | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/ListModels.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/ListModels.java) |
| List Routines | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/ListRoutines.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/ListRoutines.java) |
| List Tables | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/ListTables.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/ListTables.java) |
| Load Avro From Gcs | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadAvroFromGcs.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadAvroFromGcs.java) |
| Load Avro From Gcs Truncate | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadAvroFromGcsTruncate.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadAvroFromGcsTruncate.java) |
| Load Csv From Gcs | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadCsvFromGcs.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadCsvFromGcs.java) |
| Load Csv From Gcs Autodetect | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadCsvFromGcsAutodetect.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadCsvFromGcsAutodetect.java) |
| Load Csv From Gcs Truncate | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadCsvFromGcsTruncate.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadCsvFromGcsTruncate.java) |
| Load Json From Gcs | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadJsonFromGcs.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadJsonFromGcs.java) |
| Load Json From Gcs Autodetect | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadJsonFromGcsAutodetect.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadJsonFromGcsAutodetect.java) |
| Load Json From Gcs Cmek | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadJsonFromGcsCmek.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadJsonFromGcsCmek.java) |
| Load Json From Gcs Truncate | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadJsonFromGcsTruncate.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadJsonFromGcsTruncate.java) |
| Load Local File | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadLocalFile.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadLocalFile.java) |
| Load Orc From Gcs | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadOrcFromGcs.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadOrcFromGcs.java) |
| Load Orc From Gcs Truncate | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadOrcFromGcsTruncate.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadOrcFromGcsTruncate.java) |
| Load Parquet | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadParquet.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadParquet.java) |
| Load Parquet Replace Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadParquetReplaceTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadParquetReplaceTable.java) |
| Load Partitioned Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadPartitionedTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadPartitionedTable.java) |
| Load Table Clustered | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/LoadTableClustered.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/LoadTableClustered.java) |
| Nested Repeated Schema | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/NestedRepeatedSchema.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/NestedRepeatedSchema.java) |
| Query | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/Query.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/Query.java) |
| Query Batch | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryBatch.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryBatch.java) |
| Query Clustered Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryClusteredTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryClusteredTable.java) |
| Query Destination Table Cmek | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryDestinationTableCmek.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryDestinationTableCmek.java) |
| Query Disable Cache | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryDisableCache.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryDisableCache.java) |
| Query Dry Run | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryDryRun.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryDryRun.java) |
| Query External Gcs Perm | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryExternalGcsPerm.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryExternalGcsPerm.java) |
| Query External Gcs Temp | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryExternalGcsTemp.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryExternalGcsTemp.java) |
| Query External Sheets Perm | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryExternalSheetsPerm.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryExternalSheetsPerm.java) |
| Query External Sheets Temp | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryExternalSheetsTemp.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryExternalSheetsTemp.java) |
| Query Large Results | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryLargeResults.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryLargeResults.java) |
| Query Pagination | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryPagination.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryPagination.java) |
| Query Partitioned Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryPartitionedTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryPartitionedTable.java) |
| Query Script | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryScript.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryScript.java) |
| Query Total Rows | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryTotalRows.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryTotalRows.java) |
| Query With Array Parameters | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryWithArrayParameters.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryWithArrayParameters.java) |
| Query With Named Parameters | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryWithNamedParameters.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryWithNamedParameters.java) |
| Query With Named Types Parameters | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryWithNamedTypesParameters.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryWithNamedTypesParameters.java) |
| Query With Positional Parameters | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryWithPositionalParameters.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryWithPositionalParameters.java) |
| Query With Positional Types Parameters | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryWithPositionalTypesParameters.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryWithPositionalTypesParameters.java) |
| Query With Structs Parameters | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryWithStructsParameters.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryWithStructsParameters.java) |
| Query With Timestamp Parameters | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QueryWithTimestampParameters.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QueryWithTimestampParameters.java) |
| Quickstart Sample | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/QuickstartSample.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/QuickstartSample.java) |
| Relax Column Load Append | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/RelaxColumnLoadAppend.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/RelaxColumnLoadAppend.java) |
| Relax Column Mode | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/RelaxColumnMode.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/RelaxColumnMode.java) |
| Relax Table Query | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/RelaxTableQuery.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/RelaxTableQuery.java) |
| Run Legacy Query | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/RunLegacyQuery.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/RunLegacyQuery.java) |
| Save Query To Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/SaveQueryToTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/SaveQueryToTable.java) |
| Simple App | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/SimpleApp.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/SimpleApp.java) |
| Simple Query | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/SimpleQuery.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/SimpleQuery.java) |
| Table Exists | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/TableExists.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/TableExists.java) |
| Table Insert Rows | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/TableInsertRows.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/TableInsertRows.java) |
| Table Insert Rows Without Row Ids | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/TableInsertRowsWithoutRowIds.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/TableInsertRowsWithoutRowIds.java) |
| Undelete Table | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/UndeleteTable.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/UndeleteTable.java) |
| Update Dataset Access | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/UpdateDatasetAccess.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/UpdateDatasetAccess.java) |
| Update Dataset Description | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/UpdateDatasetDescription.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/UpdateDatasetDescription.java) |
| Update Dataset Expiration | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/UpdateDatasetExpiration.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/UpdateDatasetExpiration.java) |
| Update Dataset Partition Expiration | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/UpdateDatasetPartitionExpiration.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/UpdateDatasetPartitionExpiration.java) |
| Update Model Description | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/UpdateModelDescription.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/UpdateModelDescription.java) |
| Update Routine | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/UpdateRoutine.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/UpdateRoutine.java) |
| Update Table Cmek | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/UpdateTableCmek.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/UpdateTableCmek.java) |
| Update Table Description | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/UpdateTableDescription.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/UpdateTableDescription.java) |
| Update Table Dml | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/UpdateTableDml.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/UpdateTableDml.java) |
| Update Table Expiration | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/UpdateTableExpiration.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/UpdateTableExpiration.java) |
| Update Table Require Partition Filter | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/UpdateTableRequirePartitionFilter.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/UpdateTableRequirePartitionFilter.java) |
| Update View Query | [source code](https://github.com/googleapis/java-bigquery/blob/master/samples/snippets/src/main/java/com/example/bigquery/UpdateViewQuery.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-bigquery&page=editor&open_in_editor=samples/snippets/src/main/java/com/example/bigquery/UpdateViewQuery.java) |



## Troubleshooting

To get help, follow the instructions in the [shared Troubleshooting document][troubleshooting].

## Java Versions

Java 7 or above is required for using this client.

## Versioning


This library follows [Semantic Versioning](http://semver.org/).


## Contributing


Contributions to this library are always welcome and highly encouraged.

See [CONTRIBUTING][contributing] for more information how to get started.

Please note that this project is released with a Contributor Code of Conduct. By participating in
this project you agree to abide by its terms. See [Code of Conduct][code-of-conduct] for more
information.

## License

Apache 2.0 - See [LICENSE][license] for more information.

## CI Status

Java Version | Status
------------ | ------
Java 7 | [![Kokoro CI][kokoro-badge-image-1]][kokoro-badge-link-1]
Java 8 | [![Kokoro CI][kokoro-badge-image-2]][kokoro-badge-link-2]
Java 8 OSX | [![Kokoro CI][kokoro-badge-image-3]][kokoro-badge-link-3]
Java 8 Windows | [![Kokoro CI][kokoro-badge-image-4]][kokoro-badge-link-4]
Java 11 | [![Kokoro CI][kokoro-badge-image-5]][kokoro-badge-link-5]

[product-docs]: https://cloud.google.com/bigquery
[javadocs]: https://googleapis.dev/java/google-cloud-bigquery/latest
[kokoro-badge-image-1]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigquery/java7.svg
[kokoro-badge-link-1]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigquery/java7.html
[kokoro-badge-image-2]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigquery/java8.svg
[kokoro-badge-link-2]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigquery/java8.html
[kokoro-badge-image-3]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigquery/java8-osx.svg
[kokoro-badge-link-3]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigquery/java8-osx.html
[kokoro-badge-image-4]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigquery/java8-win.svg
[kokoro-badge-link-4]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigquery/java8-win.html
[kokoro-badge-image-5]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigquery/java11.svg
[kokoro-badge-link-5]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigquery/java11.html
[stability-image]: https://img.shields.io/badge/stability-ga-green
[maven-version-image]: https://img.shields.io/maven-central/v/com.google.cloud/google-cloud-bigquery.svg
[maven-version-link]: https://search.maven.org/search?q=g:com.google.cloud%20AND%20a:google-cloud-bigquery&core=gav
[authentication]: https://github.com/googleapis/google-cloud-java#authentication
[developer-console]: https://console.developers.google.com/
[create-project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
[cloud-sdk]: https://cloud.google.com/sdk/
[troubleshooting]: https://github.com/googleapis/google-cloud-common/blob/master/troubleshooting/readme.md#troubleshooting
[contributing]: https://github.com/googleapis/java-bigquery/blob/master/CONTRIBUTING.md
[code-of-conduct]: https://github.com/googleapis/java-bigquery/blob/master/CODE_OF_CONDUCT.md#contributor-code-of-conduct
[license]: https://github.com/googleapis/java-bigquery/blob/master/LICENSE
[enable-billing]: https://cloud.google.com/apis/docs/getting-started#enabling_billing
[enable-api]: https://console.cloud.google.com/flows/enableapi?apiid=bigquery.googleapis.com
[libraries-bom]: https://github.com/GoogleCloudPlatform/cloud-opensource-java/wiki/The-Google-Cloud-Platform-Libraries-BOM
[shell_img]: https://gstatic.com/cloudssh/images/open-btn.png
