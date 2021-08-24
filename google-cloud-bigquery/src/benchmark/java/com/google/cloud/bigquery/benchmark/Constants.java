/*
 * Copyright 2021 Google LLC
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

package com.google.cloud.bigquery.benchmark;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class Constants {
    public static final String SRC_TABLE = //"projects/bigquery-public-data/datasets/usa_names/tables/usa_1910_current";
            "projects/bigquery-public-data/datasets/new_york_taxi_trips/tables/tlc_yellow_trips_2017";

    public static  final List<String> FIELDS = ImmutableList.of("vendor_id","pickup_datetime","rate_code","dropoff_datetime","payment_type","pickup_location_id","dropoff_location_id");//,"dropoff_datetime","passenger_count","trip_distance","trip_distance","pickup_latitude","dropoff_longitude","payment_type","fare_amount","extra","tip_amount","imp_surcharge","pickup_location_id","dropoff_location_id");
    public static  final String PROJECT_ID = "java-docs-samples-testing";
    public static final int WARMUP_ITERATIONS = 2;
    public static final int MEASUREMENT_ITERATIONS = 5;
}
