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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamingMinuteTrafficSQLPipelineクラスは、
 * 分単位でWebサイトのトラフィックを集計し、BigQueryに書き込むストリーミング処理パイプラインです。
 * このパイプラインでは、Pub/SubからJSON形式のWebサーバーログを継続的に読み込み、
 * SQLを使って分単位のページビュー数を集計し、結果をBigQueryに書き込みます。
 */
public class StreamingMinuteTrafficSQLPipeline {

    /**
     * ログ出力のためのロガーです。
     * 処理の進捗やエラーなどの情報を記録するために使われます。
     */
    private static final Logger LOG = LoggerFactory.getLogger(StreamingMinuteTrafficSQLPipeline.class);

    /**
     * Optionsインターフェースは、コマンドラインで実行時に渡されるカスタム実行オプションを定義します。
     * パイプラインの実行に必要なパラメータ (入力トピック名、BigQueryのテーブル名など) を指定するために使われます。
     */
    public interface Options extends DataflowPipelineOptions, BeamSqlPipelineOptions {
        @Description("入力Pub/Subトピック名")
        String getInputTopic();
        void setInputTopic(String inputTopic);

        @Description("集計結果を書き込むBigQueryのテーブル名")
        String getTableName();
        void setTableName(String tableName);
    }

    /**
     * パイプライン実行のためのメインのエントリポイントです。
     * このメソッドはパイプラインを開始しますが、実行が終了するまで待機しません。
     * プログラムの実行をブロックして待機する必要がある場合は、{@link StreamingMinuteTrafficSQLPipeline#run(Options)} メソッドを使用して
     * パイプラインを開始し、{@link PipelineResult} の {@code result.waitUntilFinish()} を呼び出してください。
     *
     * @param args 実行時に渡されるコマンドライン引数。
     */
    public static void main(String args) {
        // 1. オプションの登録と設定
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class); // コマンドライン引数からオプションを取得します。
        options.setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner"); // SQLプランナーを設定します。
        run(options); // パイプラインを実行します。
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

    // 2. スキーマの定義
    /**
     * jodaCommonLogSchemaは、ログデータにDateTime型の `timestamp_joda` フィールドを追加したスキーマです。
     * Beam SQLで時間ベースのウィンドウ処理を行うために、DateTime型のフィールドが必要となります。
     */
    public static final Schema jodaCommonLogSchema = Schema.builder()
          .addStringField("user_id") // ユーザーID
          .addStringField("ip") // IPアドレス
          .addDoubleField("lat") // 緯度
          .addDoubleField("lng") // 経度
          .addStringField("timestamp") // タイムスタンプ (文字列型)
          .addStringField("http_request") // HTTPリクエスト
          .addStringField("user_agent") // ユーザーエージェント
          .addInt64Field("http_response") // HTTPレスポンスコード
          .addInt64Field("num_bytes") // レスポンスのバイト数
          .addDateTimeField("timestamp_joda") // タイムスタンプ (DateTime型)
          .build();

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

        // 3. パイプラインの作成
        Pipeline pipeline = Pipeline.create(options); // パイプラインを作成します。
        options.setJobName("streaming-minute-traffic-sql-pipeline-" + System.currentTimeMillis()); // ジョブ名を設定します。

        /*
         * Steps:
         * 1) Read something
         * 2) Transform something
         * 3) Write something
         */

        // 4. データの読み込み、変換、ウィンドウ処理、集計、BigQueryへの書き込み
        pipeline
                // Read in lines from PubSub and Parse to CommonLog
              .apply("ReadMessage", PubsubIO.readStrings() // Pub/SubからJSON形式のログデータを読み込みます。
                      .withTimestampAttribute("timestamp") // タイムスタンプ属性を設定します。
                      .fromTopic(options.getInputTopic())) // 入力トピックを指定します。
              .apply("ParseJson", ParDo.of(new JsonToCommonLog())) // JSONデータをCommonLogオブジェクトに変換します。

                // Add new DATETIME field to CommonLog, converting to a Row, then populate new row with Joda DateTime
              .apply("AddDateTimeField", AddFields.<CommonLog>create().field("timestamp_joda", FieldType.DATETIME)) // スキーマにDateTimeフィールド `timestamp_joda` を追加します。
              .apply("AddDateTimeValue", MapElements.via(new SimpleFunction<Row, Row>() { // DateTimeフィールドに値を設定します。
                    @Override
                    public Row apply(Row row) {
                        DateTime dateTime = new DateTime(row.getString("timestamp")); // 文字列型の `timestamp` を DateTime型に変換します。
                        // 新しいスキーマでRowを作成し、値を設定します。
                        return Row.withSchema(row.getSchema())
                              .addValues(
                                        row.getString("user_id"), // ユーザーID
                                        row.getString("ip"), // IPアドレス
                                        row.getDouble("lat"), // 緯度
                                        row.getDouble("lng"), // 経度
                                        row.getString("timestamp"), // タイムスタンプ (文字列型)
                                        row.getString("http_request"), // HTTPリクエスト
                                        row.getString("user_agent"), // ユーザーエージェント
                                        row.getInt64("http_response"), // HTTPレスポンスコード
                                        row.getInt64("num_bytes"), // レスポンスのバイト数
                                        dateTime) // DateTime型の `timestamp_joda`
                              .build();
                    }
                })).setRowSchema(jodaCommonLogSchema) // スキーマを設定します。

                // Apply a SqlTransform.query(QUERY_TEXT) to count window and count total page views, write to BQ
              .apply("WindowedAggregateQuery", SqlTransform.query( // SQLでウィンドウ処理と集計を行います。
                        "SELECT COUNT(*) AS pageviews, tr.window_start AS minute FROM TUMBLE( ( SELECT * FROM " +
                                "PCOLLECTION ), DESCRIPTOR(timestamp_joda), \"INTERVAL 1 MINUTE\") AS tr GROUP " +
                                "BY tr.window_start")) // `timestamp_joda` を基準に1分間のウィンドウで集計し、ページビュー数とウィンドウの開始時刻を取得します。
              .apply("WriteToBQ",
                        BigQueryIO.<Row>write().to(options.getTableName()).useBeamSchema() // BigQueryに書き込みます。
                              .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND) // 新しいデータを追加します。
                              .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)); // テーブルがなければ作成します。

        LOG.info("Building pipeline...");

        return pipeline.run(); // パイプラインを実行します。
    }
}