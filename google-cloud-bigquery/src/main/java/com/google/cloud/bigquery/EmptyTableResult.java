/*
 * Copyright 2018 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.cloud.PageImpl;
import javax.annotation.Nullable;

public class EmptyTableResult extends TableResult {

  private static final long serialVersionUID = -4831062717210349819L;

  /** An empty {@code TableResult} to avoid making API requests to unlistable tables. */
  @InternalApi("Exposed for testing")
  public EmptyTableResult(@Nullable Schema schema) {
    super(schema, 0, new PageImpl<FieldValueList>(null, "", null), null);
  }
}
