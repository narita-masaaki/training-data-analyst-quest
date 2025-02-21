# 1. モジュールのインポート
import argparse  # コマンドライン引数を扱うためのモジュール
import time  # 時間に関する処理を行うモジュール
import logging  # ログ出力を行うモジュール
import json  # JSON データを扱うモジュール
import apache_beam as beam  # Apache Beam の主要モジュール
from apache_beam.options.pipeline_options import GoogleCloudOptions  # Google Cloud 関連のオプション
from apache_beam.options.pipeline_options import PipelineOptions  # Beam パイプラインのオプション
from apache_beam.options.pipeline_options import StandardOptions  # Beam の標準オプション
from apache_beam.runners import DataflowRunner, DirectRunner  # Beam パイプラインの実行環境

# この部分では、プログラムで使用する様々な機能をまとめた「モジュール」を読み込んでいます。
# モジュールは、便利な道具箱のようなもので、それぞれのモジュールが特定の機能を提供してくれます。
# 例えば、"json" モジュールは JSON データを扱うための機能を提供し、"apache_beam" モジュールは Apache Beam というデータ処理フレームワークの機能を提供します。

# 2. 関数の定義
# JSON データを Python の辞書型に変換する関数
def parse_json(element):
    return json.loads(element)

# 特定のフィールドを削除する関数
def drop_fields(element):
    element.pop('user_agent')  # 'user_agent' フィールドを削除
    return element

# ここでは、2つの関数を定義しています。
# 関数は、特定の処理をまとめたもので、必要に応じて呼び出すことができます。
# "parse_json" 関数は、JSON 形式の文字列を Python の辞書型オブジェクトに変換します。
# "drop_fields" 関数は、辞書型オブジェクトから "user_agent" というキーを持つ要素を削除します。

# 3. メイン処理を行う関数
def run():
    # 3.1. コマンドライン引数の解析
    parser = argparse.ArgumentParser(description='JsonからBigQueryへのロード')  # パーサーを作成
    # 必須の引数
    parser.add_argument('--project', required=True, help='Google Cloudプロジェクトを指定')
    parser.add_argument('--region', required=True, help='Google Cloudリージョンを指定')
    parser.add_argument('--runner', required=True, help='Apache Beam Runnerを指定')
    parser.add_argument('--inputPath', required=True, help='events.jsonへのパス')
    parser.add_argument('--outputPath', required=True, help='coldlineストレージバケットへのパス')
    parser.add_argument('--tableName', required=True, help='BigQueryテーブル名')

    opts, pipeline_opts = parser.parse_known_args()  # 引数を解析

    # この部分では、コマンドライン引数を解析しています。
    # コマンドライン引数とは、プログラムを実行する際に指定する追加の情報のことです。
    # 例えば、入力ファイルのパスや出力先のバケット名などをコマンドライン引数として指定することができます。
    # "argparse" モジュールを使うと、簡単にコマンドライン引数を処理することができます。

    # 3.2. Beamパイプラインオプションの設定
    options = PipelineOptions(pipeline_opts)  # オプションを作成
    # Google Cloud 関連のオプション
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-', time.time_ns())  # ジョブ名
    # Runner の指定 (DirectRunner: ローカル, DataflowRunner: Google Cloud)
    options.view_as(StandardOptions).runner = opts.runner

    # ここでは、Apache Beam パイプラインのオプションを設定しています。
    # パイプラインとは、データの処理手順をまとめたもので、Beam ではパイプラインを使って様々なデータ処理を行います。
    # オプションには、プロジェクト ID やリージョン、ジョブ名、Runner などを設定することができます。
    # Runner は、パイプラインをどこで実行するかを指定するもので、ローカル環境で実行する "DirectRunner" や Google Cloud Platform 上で実行する "DataflowRunner" などがあります。

    # 3.3. 各種変数の設定
    input_path = opts.inputPath  # 入力パス
    output_path = opts.outputPath  # 出力パス
    table_name = opts.tableName  # テーブル名

    # コマンドライン引数から取得した入力ファイルのパス、出力先のバケット名、BigQuery テーブル名などの値を
    # それぞれの変数に格納しています。

    # 3.4. BigQueryのテーブルスキーマ
    table_schema = {  # スキーマを定義
        "fields": [
            {"name": "ip", "type": "STRING"},
            {"name": "user_id", "type": "STRING"},
            {"name": "lat", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "lng", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "STRING"},
            {"name": "http_request", "type": "STRING"},
            {"name": "http_response", "type": "INTEGER"},
            {"name": "num_bytes", "type": "INTEGER"}
        ]
    }

    # BigQuery にデータを書き込む際に、データの形式（スキーマ）を定義する必要があります。
    # ここでは、各フィールドの名前や型などを指定しています。

    # 3.5. パイプラインの作成
    p = beam.Pipeline(options=options)  # パイプラインを作成

    # Apache Beam では、データを処理する手順をパイプラインとして表現します。
    # ここでは、"beam.Pipeline" を使ってパイプラインを作成しています。

    # 3.6. データの読み込み、GCSへの書き込み
    lines = p | 'ReadFromGCS' >> beam.io.ReadFromText(input_path)  # GCSから読み込み
    lines | 'WriteRawToGCS' >> beam.io.WriteToText(output_path)  # GCSへ書き込み

    # この部分では、Google Cloud Storage (GCS) からデータを読み込み、別の GCS の場所にデータを書き込んでいます。
    # "beam.io.ReadFromText" でテキストファイルを読み込み、"beam.io.WriteToText" でテキストファイルに書き込んでいます。
    # "p | 'ReadFromGCS' >> ..." という記法は、パイプラインに処理を追加する際に使われます。

    # 3.7. データの変換、フィルタリング、BigQueryへの書き込み
    (
        lines  # 読み込んだデータ
        | 'ParseJson' >> beam.Map(parse_json)  # JSON文字列を辞書に変換
        | 'DropFields' >> beam.Map(drop_fields)  # 不要なフィールドを削除
        | 'FilterFn' >> beam.Filter(lambda row: row['num_bytes'] < 120)  # 条件でフィルタリング
        | 'WriteToBQ' >> beam.io.WriteToBigQuery(  # BigQueryへ書き込み
            table_name,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  # テーブルがなければ作成
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE  # 既存のテーブルを上書き
        )
    )

    # この部分では、読み込んだデータを変換、フィルタリングし、BigQuery に書き込んでいます。
    # "beam.Map" は、各要素に対して関数を適用する処理です。
    # "beam.Filter" は、条件に合致する要素のみを抽出する処理です。
    # "beam.io.WriteToBigQuery" は、BigQuery にデータを書き込む処理です。

    # 3.8. ログ設定とパイプライン実行
    logging.getLogger().setLevel(logging.INFO)  # ログレベルを設定
    logging.info("パイプラインを構築中 ...")  # ログ出力
    p.run()  # パイプラインを実行

    # 最後に、ログの設定を行い、パイプラインを実行しています。
    # "p.run()" を実行することで、定義したパイプラインが実際に動作し、データ処理が行われます。

# 4. メイン処理の実行
if __name__ == '__main__':
    run()

# この部分は、プログラムが直接実行された場合に "run" 関数を呼び出すためのものです。
# Python では、スクリプトが直接実行された場合と、別のスクリプトからモジュールとして読み込まれた場合で、
# 異なる処理を行うことができます。
# "if __name__ ==
