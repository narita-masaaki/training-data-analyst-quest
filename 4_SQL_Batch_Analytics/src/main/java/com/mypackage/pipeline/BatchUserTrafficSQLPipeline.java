/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mypackage.pipeline;

import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BatchUserTrafficSQLPipeline {

    /**
     * ログ出力のためのロガーです。
     * 処理の進捗やエラーなどの情報を記録するために使われます。
     */
    private static final Logger LOG = LoggerFactory.getLogger(BatchUserTrafficSQLPipeline.class);

    /**
     * {@link Options} インターフェースは、コマンドラインで実行時に渡されるカスタム実行オプションを定義します。
     * パイプラインの実行に必要なパラメータ (入力ファイルのパス、BigQueryのテーブル名など) を指定するために使われます。
     */
    public interface Options extends DataflowPipelineOptions, BeamSqlPipelineOptions {
        @Description("イベントデータ (events.json) が格納されているGCSのバケットのパス")
        String getInputPath();
        void setInputPath(String inputPath);

        @Description("集計結果を書き込むBigQueryのテーブル名")
        String getAggregateTableName();
        void setAggregateTableName(String aggregateTableName);

        @Description("生のログデータを書き込むBigQueryのテーブル名")
        String getRawTableName();
        void setRawTableName(String rawTableName);
    }

    /**
     * JsonToCommonLogクラスは、JSON形式の文字列を受け取り、CommonLogオブジェクトに変換するDoFnです。
     * DoFnは、Apache Beamにおけるデータ処理の基本単位であり、入力を受け取り、処理を行い、出力を生成します。
     */
    static class JsonToCommonLog extends DoFn<String, CommonLog> {
        @ProcessElement
        public void processElement(@Element String json, OutputReceiver<CommonLog> r) throws Exception {
            Gson gson = new Gson();
            CommonLog commonLog = gson.fromJson(json, CommonLog.class); // Gsonを使ってJSONをCommonLogオブジェクトに変換します。
            r.output(commonLog); // 変換したCommonLogオブジェクトを出力します。
        }
    }

    /**
     * パイプライン実行のためのメインのエントリポイントです。
     * このメソッドはパイプラインを開始しますが、実行が終了するまで待機しません。
     * プログラムの実行をブロックして待機する必要がある場合は、{@link BatchUserTrafficSQLPipeline#run(Options)} メソッドを使用して
     * パイプラインを開始し、{@link PipelineResult} の {@code result.waitUntilFinish()} を呼び出してください。
     *
     * @param args 実行時に渡されるコマンドライン引数。
     */
    public static void main(String args) {
        // 1. オプションの登録と設定
        PipelineOptionsFactory.register(Options.class); // Options クラスを登録します。
        Options options = PipelineOptionsFactory.fromArgs(args)
              .withValidation()
              .as(Options.class); // コマンドライン引数からオプションを取得します。
        options.setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner"); // SQLプランナーを設定します。
        run(options); // パイプラインを実行します。
    }

    /**
     * 指定されたオプションでパイプラインを実行します。
     * このメソッドは、パイプラインが終了するまで待機しません。
     * プログラムの実行をブロックして待機する必要がある場合は、
     * 結果オブジェクトの {@code result.waitUntilFinish()} を呼び出して、
     * パイプラインの実行が終了するまでブロックしてください。
     *
     * @param options 実行オプション。
     * @return パイプラインの結果。
     */
    public static PipelineResult run(Options options) {

        // 2. パイプラインの作成
        Pipeline pipeline = Pipeline.create(options); // パイプラインを作成します。
        options.setJobName("batch-user-traffic-sql-pipeline-" + System.currentTimeMillis()); // ジョブ名を設定します。

        /*
         * Steps:
         * 1) Read something
         * 2) Transform something
         * 3) Write something
         */

        // 3. データの読み込みと変換
        PCollection<CommonLog> logs = pipeline
                // Read in lines from GCS and Parse to CommonLog
              .apply("ReadFromGCS", TextIO.read().from(options.getInputPath())) // GCSからデータを読み込みます。
              .apply("ParseJson", ParDo.of(new JsonToCommonLog())); // JSON形式のデータをCommonLogオブジェクトに変換します。

        // 4. 集計クエリの適用とBigQueryへの書き込み
        logs
                // Apply a SqlTransform.query(QUERY_TEXT) to count page views and other aggregations
              .apply("AggregateSQLQuery", SqlTransform.query( // SQLで集計クエリを実行します。
                        "SELECT user_id, COUNT(*) AS pageviews, SUM(num_bytes) as total_bytes, MAX(num_bytes) AS max_num_bytes, MIN(num_bytes) as min_num_bytes " +
                                "FROM PCOLLECTION GROUP BY user_id")) // ユーザーIDごとにページビュー数、合計バイト数、最大バイト数、最小バイト数を集計します。
              .apply("WriteAggregateToBQ",
                        BigQueryIO.<Row>write().to(options.getAggregateTableName()).useBeamSchema() // 集計結果をBigQueryに書き込みます。
                              .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE) // テーブルがあれば切り捨てます。
                              .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)); // テーブルがなければ作成します。


        // 5. 生ログのBigQueryへの書き込み
        logs
                // Write the raw logs to BigQuery
              .apply("WriteRawToBQ",
                        BigQueryIO.<CommonLog>write().to(options.getRawTableName()).useBeamSchema() // 生のログデータをBigQueryに書き込みます。
                              .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE) // テーブルがあれば切り捨てます。
                              .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)); // テーブルがなければ作成します。

        LOG.info("Building pipeline...");

        return pipeline.run(); // パイプラインを実行します。
    }
}