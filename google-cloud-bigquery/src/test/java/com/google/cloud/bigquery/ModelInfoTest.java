/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.api.services.bigquery.model.TrainingOptions;
import com.google.api.services.bigquery.model.TrainingRun;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ModelInfoTest {

  private static final ModelId MODEL_ID = ModelId.of("dataset", "model");
  private static final String ETAG = "etag";
  private static final Long CREATION_TIME = 10L;
  private static final Long LAST_MODIFIED_TIME = 20L;
  private static final Long EXPIRATION_TIME = 30L;
  private static final String DESCRIPTION = "description";
  private static final String FRIENDLY_NAME = "friendlyname";
  private static final String LOCATION = "US";
  private static final EncryptionConfiguration MODEL_ENCRYPTION_CONFIGURATION =
      EncryptionConfiguration.newBuilder().setKmsKeyName("KMS_KEY_1").build();

  private static final TrainingOptions TRAINING_OPTIONS =
      new TrainingOptions().setDataSplitColumn("foo").setEarlyStop(true).setLossType("bar");
  private static final TrainingRun TRAINING_RUN =
      new TrainingRun().setTrainingOptions(TRAINING_OPTIONS);
  private static final List<TrainingRun> TRAINING_RUN_LIST =
      Collections.singletonList(TRAINING_RUN);

  private static final ModelInfo MODEL_INFO =
      ModelInfo.newBuilder(MODEL_ID)
          .setEtag(ETAG)
          .setCreationTime(CREATION_TIME)
          .setExpirationTime(EXPIRATION_TIME)
          .setLastModifiedTime(LAST_MODIFIED_TIME)
          .setDescription(DESCRIPTION)
          .setFriendlyName(FRIENDLY_NAME)
          .setTrainingRuns(TRAINING_RUN_LIST)
          .setEncryptionConfiguration(MODEL_ENCRYPTION_CONFIGURATION)
          .setLocation(LOCATION)
          .build();

  @Test
  void testToBuilder() {
    compareModelInfo(MODEL_INFO.toBuilder().build());
  }

  @Test
  void testToBuilderIncomplete() {
    ModelInfo modelInfo = ModelInfo.of(MODEL_ID);
    assertEquals(modelInfo, modelInfo.toBuilder().build());
  }

  @Test
  void testBuilder() {
    assertEquals(ETAG, MODEL_INFO.getEtag());
    assertEquals(CREATION_TIME, MODEL_INFO.getCreationTime());
    assertEquals(LAST_MODIFIED_TIME, MODEL_INFO.getLastModifiedTime());
    assertEquals(EXPIRATION_TIME, MODEL_INFO.getExpirationTime());
    assertEquals(DESCRIPTION, MODEL_INFO.getDescription());
    assertEquals(FRIENDLY_NAME, MODEL_INFO.getFriendlyName());
    assertEquals(TRAINING_OPTIONS, MODEL_INFO.getTrainingRuns().get(0).getTrainingOptions());
    assertEquals(MODEL_ENCRYPTION_CONFIGURATION, MODEL_INFO.getEncryptionConfiguration());
    assertEquals(LOCATION, MODEL_INFO.getLocation());
  }

  @Test
  void testOf() {
    ModelInfo modelInfo = ModelInfo.of(MODEL_ID);
    assertEquals(MODEL_ID, modelInfo.getModelId());
    assertNull(modelInfo.getEtag());
    assertNull(modelInfo.getCreationTime());
    assertNull(modelInfo.getLastModifiedTime());
    assertNull(modelInfo.getExpirationTime());
    assertNull(modelInfo.getDescription());
    assertNull(modelInfo.getFriendlyName());
    assertNull(modelInfo.getEncryptionConfiguration());
    assertNull(modelInfo.getLocation());
    assertTrue(modelInfo.getTrainingRuns().isEmpty());
    assertTrue(modelInfo.getLabelColumns().isEmpty());
    assertTrue(modelInfo.getFeatureColumns().isEmpty());
  }

  @Test
  void testToAndFromPb() {
    compareModelInfo(ModelInfo.fromPb(MODEL_INFO.toPb()));
  }

  @Test
  void testSetProjectId() {
    assertEquals("project", MODEL_INFO.setProjectId("project").getModelId().getProject());
  }

  private void compareModelInfo(ModelInfo value) {
    assertEquals(ModelInfoTest.MODEL_INFO, value);
    assertEquals(ModelInfoTest.MODEL_INFO.getModelId(), value.getModelId());
    assertEquals(ModelInfoTest.MODEL_INFO.getEtag(), value.getEtag());
    assertEquals(ModelInfoTest.MODEL_INFO.getCreationTime(), value.getCreationTime());
    assertEquals(ModelInfoTest.MODEL_INFO.getLastModifiedTime(), value.getLastModifiedTime());
    assertEquals(ModelInfoTest.MODEL_INFO.getExpirationTime(), value.getExpirationTime());
    assertEquals(ModelInfoTest.MODEL_INFO.getDescription(), value.getDescription());
    assertEquals(ModelInfoTest.MODEL_INFO.getFriendlyName(), value.getFriendlyName());
    assertEquals(ModelInfoTest.MODEL_INFO.getLabels(), value.getLabels());
    assertEquals(ModelInfoTest.MODEL_INFO.getLocation(), value.getLocation());
    assertEquals(ModelInfoTest.MODEL_INFO.hashCode(), value.hashCode());
    assertEquals(ModelInfoTest.MODEL_INFO.getTrainingRuns(), value.getTrainingRuns());
    assertEquals(ModelInfoTest.MODEL_INFO.getLabelColumns(), value.getLabelColumns());
    assertEquals(ModelInfoTest.MODEL_INFO.getFeatureColumns(), value.getFeatureColumns());
    assertEquals(
        ModelInfoTest.MODEL_INFO.getEncryptionConfiguration(), value.getEncryptionConfiguration());
  }
}
