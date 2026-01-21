#!/bin/bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ${DIR}/../google-cloud-bigquery-jdbc

FOLDER=release
DATE=$(date '+%Y-%m-%d')
COMMIT=$(git rev-parse HEAD)
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
BUCKET=gs://bq_devtools_release_private/drivers/jdbc
NIGHTLY_BUILD_DESTINATION=${BUCKET}/nightly/${VERSION}/${DATE}

# All dependencies release
mkdir -p ./${FOLDER}
make docker-package-all-dependencies PACKAGE_DESTINATION=$(pwd)/${FOLDER}
JAR_FILE=$(find . -wholename "./${FOLDER}/*jar" -print -quit)
JAR_NAME=$(basename ${JAR_FILE} .jar)-${DATE}-${COMMIT}

gsutil cp ${JAR_FILE} "${NIGHTLY_BUILD_DESTINATION}/${JAR_NAME}.jar"
rm -rf ${FOLDER}

# All dependencies release - shaded
mkdir -p ./${FOLDER}
make docker-package-all-dependencies-shaded PACKAGE_DESTINATION=$(pwd)/${FOLDER}
JAR_FILE=$(find . -wholename "./${FOLDER}/*jar" -print -quit)

gsutil cp ${JAR_FILE} "${NIGHTLY_BUILD_DESTINATION}/${JAR_NAME}-shaded.jar"
rm -rf ${FOLDER}

# Thin release
mkdir -p ./${FOLDER}
make docker-package PACKAGE_DESTINATION=$(pwd)/${FOLDER}
ZIP_FILE=$(find . -wholename "./${FOLDER}/*zip" -print -quit)

gsutil cp ${ZIP_FILE} "${NIGHTLY_BUILD_DESTINATION}/${JAR_NAME}.zip"
rm -rf ${FOLDER}

# Update latest version
gsutil cp "${NIGHTLY_BUILD_DESTINATION}/${JAR_NAME}.zip" "${BUCKET}/google-cloud-bigquery-jdbc-latest.zip"
gsutil cp "${NIGHTLY_BUILD_DESTINATION}/${JAR_NAME}.jar" "${BUCKET}/google-cloud-bigquery-jdbc-latest-full.jar"
gsutil cp "${NIGHTLY_BUILD_DESTINATION}/${JAR_NAME}-shaded.jar" "${BUCKET}/google-cloud-bigquery-jdbc-latest-full-shaded.jar"
