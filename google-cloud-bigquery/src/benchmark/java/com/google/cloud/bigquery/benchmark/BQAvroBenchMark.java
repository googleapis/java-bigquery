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


import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableModifiers;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.common.base.Preconditions;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Fork(value = 1)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = Constants.WARMUP_ITERATIONS)
@Measurement(iterations = Constants.MEASUREMENT_ITERATIONS)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)


public class BQAvroBenchMark {
    private static BigQueryReadClient client;

    private static SimpleRowReader reader;
    private  List<AvroRows> cachedRowRes = null;
    private Schema avroSchema;

    private static class SimpleRowReader {

        private final DatumReader<GenericRecord> datumReader;

        // Decoder object will be reused to avoid re-allocation and too much garbage collection.
        private BinaryDecoder decoder = null;

        // GenericRecord object will be reused.
        private GenericRecord row = null;

        public SimpleRowReader(Schema schema) {
            Preconditions.checkNotNull(schema);
            datumReader = new GenericDatumReader<>(schema);

        }

        /**
         * Sample method for processing AVRO rows which only validates decoding.
         *
         * @param avroRows object returned from the ReadRowsResponse.
         */
        public void processRows(AvroRows avroRows, Blackhole blackhole) throws IOException {////deserialize the values and consume the hash of the values
            decoder =
                    DecoderFactory.get()
                            .binaryDecoder(avroRows.getSerializedBinaryRows().toByteArray(), decoder);
            blackhole.consume(avroRows);
            while (!decoder.isEnd()) {
                // Reusing object row
                row = datumReader.read(row, decoder);
                long hash = 0;
                for(String field: Constants.FIELDS){
                    hash += row.get(field)!=null? row.get(field).toString().hashCode():0;////get to the row level value as String and compute the hashcode
                }
                blackhole.consume(hash);
            }

        }

    }


    @Setup
    public void setUp() {
        try {
            if(this.cachedRowRes == null) {
                this.cachedRowRes = getRowsFromBQStorage();//cache the BQ rows to avoid any variation in benchmarking due to network factors
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    private List<AvroRows> getRowsFromBQStorage() throws IOException {


        this.client = BigQueryReadClient.create();
        List<AvroRows> cachedRowRes = new ArrayList<>();
        Integer snapshotMillis = null;
        String[] args = {null};
        if (args.length > 1) {
            snapshotMillis = Integer.parseInt(args[1]);
        }

        String parent = String.format("projects/%s", Constants.PROJECT_ID);

        TableReadOptions options =
                TableReadOptions.newBuilder()
                        .addAllSelectedFields(Constants.FIELDS)
                        .build();

        // Start specifying the read session we want created.
        ReadSession.Builder sessionBuilder =
                ReadSession.newBuilder()
                        .setTable(Constants.SRC_TABLE)
                        // This API can also deliver data serialized in Apache Avro format.
                        // This example leverages Apache Avro.
                        .setDataFormat(DataFormat.AVRO)
                        .setReadOptions(options);

        // Optionally specify the snapshot time.  When unspecified, snapshot time is "now".
        if (snapshotMillis != null) {
            Timestamp t =
                    Timestamp.newBuilder()
                            .setSeconds(snapshotMillis / 1000)
                            .setNanos((int) ((snapshotMillis % 1000) * 1000000))
                            .build();
            TableModifiers modifiers = TableModifiers.newBuilder().setSnapshotTime(t).build();
            sessionBuilder.setTableModifiers(modifiers);
        }

        // Begin building the session creation request.
        CreateReadSessionRequest.Builder builder =
                CreateReadSessionRequest.newBuilder()
                        .setParent(parent)
                        .setReadSession(sessionBuilder)
                        .setMaxStreamCount(1);


        // Request the session creation.
        ReadSession session = client.createReadSession(builder.build());


        avroSchema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
        // Assert that there are streams available in the session.  An empty table may not have
        // data available.  If no sessions are available for an anonymous (cached) table, consider
        // writing results of a query to a named table rather than consuming cached results directly.
        Preconditions.checkState(session.getStreamsCount() > 0);

        // Use the first stream to perform reading.
        String streamName = session.getStreams(0).getName();

        ReadRowsRequest readRowsRequest =
                ReadRowsRequest.newBuilder().setReadStream(streamName).build();

        // Process each block of rows as they arrive and decode using our simple row reader.
        ServerStream<ReadRowsResponse> stream = client.readRowsCallable().call(readRowsRequest);
        reader =
                new SimpleRowReader(avroSchema);
        for (ReadRowsResponse response : stream) {
            Preconditions.checkState(response.hasAvroRows());
            cachedRowRes.add(response.getAvroRows());
        }
        return cachedRowRes;
    }

    @Benchmark
    public void deserializationBenchmark( Blackhole blackhole) throws Exception {

        // System.out.println("cachedRowRes.size(): "+cachedRowRes.size());
        for (AvroRows res: cachedRowRes){
            reader.processRows(res, blackhole);
        }

    }


    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(BQAvroBenchMark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();

    }
}