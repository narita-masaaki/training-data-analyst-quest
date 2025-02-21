# 1. モジュールのインポート
import argparse  # コマンドライン引数を扱うためのモジュール
import time  # 時間に関する処理を行うモジュール
import logging  # ログ出力を行うモジュール
import json  # JSON データを扱うモジュール
import typing  # 型ヒントを扱うモジュール
from datetime import datetime  # 日付と時刻を扱うモジュール
import apache_beam as beam  # Apache Beam の主要モジュール
from apache_beam.options.pipeline_options import GoogleCloudOptions  # Google Cloud 関連のオプション
from apache_beam.options.pipeline_options import PipelineOptions  # Beam パイプラインのオプション
from apache_beam.options.pipeline_options import StandardOptions  # Beam の標準オプション
from apache_beam.transforms.combiners import CountCombineFn  # 集計処理を行うためのモジュール
from apache_beam.runners import DataflowRunner, DirectRunner  # Beam パイプラインの実行環境

# この部分では、プログラムで使用する様々な機能をまとめた「モジュール」を読み込んでいます。
# モジュールは、便利な道具箱のようなもので、それぞれのモジュールが特定の機能を提供してくれます。
# 例えば、"json" モジュールは JSON データを扱うための機能を提供し、"apache_beam" モジュールは Apache Beam というデータ処理フレームワークの機能を提供します。


# 2. クラスの定義
class CommonLog(typing.NamedTuple):
    ip: str
    user_id: str
    lat: float
    lng: float
    timestamp: str
    http_request: str
    http_response: int
    num_bytes: int
    user_agent: str

beam.coders.registry.register_coder(CommonLog, beam.coders.RowCoder)

# ここでは、データを扱いやすくするための「クラス」を定義しています。
# クラスは、データの構造を定義するための設計図のようなものです。
# "CommonLog" クラスは、ログデータの構造を定義しています。
# "beam.coders.registry.register_coder" は、Beam でデータを処理する際に、どのようにデータをエンコード/デコードするかを指定するためのものです。


# 3. 関数の定義
def parse_json(element):
    row = json.loads(element)
    return CommonLog(**row)

def add_timestamp(element):
    ts = datetime.strptime(element.timestamp[:-8], "%Y-%m-%dT%H:%M:%S").timestamp()
    return beam.window.TimestampedValue(element, ts)

# ここでは、2つの関数を定義しています。
# 関数は、特定の処理をまとめたもので、必要に応じて呼び出すことができます。
# "parse_json" 関数は、JSON 形式の文字列を "CommonLog" クラスのオブジェクトに変換します。
# "add_timestamp" 関数は、ログデータにタイムスタンプを追加します。


# 4. クラスの定義
class GetTimestampFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().strftime("%Y-%m-%dT%H:%M:%S")
        output = {'page_views': element, 'timestamp': window_start}
        yield output

# ここでは、"GetTimestampFn" というクラスを定義しています。
# このクラスは、ウィンドウの開始時刻を取得し、出力データにタイムスタンプを追加するためのものです。
# ウィンドウとは、データを時間ごとに分割するための仕組みです。


# 5. メイン処理を行う関数
def run():
    # 5.1. コマンドライン引数の解析
    parser = argparse.ArgumentParser(description='Load from Json into BigQuery')  # パーサーを作成
    # 必須の引数
    parser.add_argument('--project', required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--input_path', required=True, help='Path to events.json')
    parser.add_argument('--table_name', required=True, help='BigQuery table name')

    opts = parser.parse_args()  # 引数を解析

    # この部分では、コマンドライン引数を解析しています。
    # コマンドライン引数とは、プログラムを実行する際に指定する追加の情報のことです。
    # 例えば、入力ファイルのパスや出力先のテーブル名などをコマンドライン引数として指定することができます。
    # "argparse" モジュールを使うと、簡単にコマンドライン引数を処理することができます。


    # 5.2. Beamパイプラインオプションの設定
    options = PipelineOptions(save_main_session=True)  # オプションを作成
    # Google Cloud 関連のオプション
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('batch-minute-traffic-pipeline-', time.time_ns())  # ジョブ名
    # Runner の指定 (DirectRunner: ローカル, DataflowRunner: Google Cloud)
    options.view_as(StandardOptions).runner = opts.runner

    # ここでは、Apache Beam パイプラインのオプションを設定しています。
    # パイプラインとは、データの処理手順をまとめたもので、Beam ではパイプラインを使って様々なデータ処理を行います。
    # オプションには、プロジェクト ID やリージョン、ジョブ名、Runner などを設定することができます。
    # Runner は、パイプラインをどこで実行するかを指定するもので、ローカル環境で実行する "DirectRunner" や Google Cloud Platform 上で実行する "DataflowRunner" などがあります。


    # 5.3. 入力と出力の設定
    input_path = opts.input_path  # 入力パス
    table_name = opts.table_name  # テーブル名

    # ここでは、入力ファイルのパスと出力先の BigQuery テーブルを指定しています。


    # 5.4. BigQueryのテーブルスキーマ
    table_schema = {  # スキーマを定義
        "fields": [
            {"name": "page_views", "type": "INTEGER"},
            {"name": "timestamp", "type": "STRING"}
        ]
    }

    # BigQuery にデータを書き込む際に、データの形式（スキーマ）を定義する必要があります。
    # ここでは、各フィールドの名前や型などを指定しています。


    # 5.5. パイプラインの作成
    p = beam.Pipeline(options=options)  # パイプラインを作成

    # Apache Beam では、データを処理する手順をパイプラインとして表現します。
    # ここでは、"beam.Pipeline" を使ってパイプラインを作成しています。


    # 5.6. データの読み込み、変換、ウィンドウ処理、集計、BigQueryへの書き込み
    (p | 'ReadFromGCS' >> beam.io.ReadFromText(input_path)  # GCSから読み込み
       | 'ParseJson' >> beam.Map(parse_json).with_output_types(CommonLog)  # JSONデータを解析
       | 'AddEventTimestamp' >> beam.Map(add_timestamp)  # タイムスタンプを追加
       | "WindowByMinute" >> beam.WindowInto(beam.window.FixedWindows(60))  # 1分間のウィンドウに分割
       | "CountPerMinute" >> beam.CombineGlobally(CountCombineFn()).without_defaults()  # 1分ごとの件数をカウント
       | "AddWindowTimestamp" >> beam.ParDo(GetTimestampFn())  # ウィンドウの開始時刻を追加
       | 'WriteToBQ' >> beam.io.WriteToBigQuery(  # BigQueryへ書き込み
            table_name,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  # テーブルがなければ作成
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE  # 既存のテーブルを上書き
            )
    )

    # この部分では、Google Cloud Storage (GCS) からデータを読み込み、JSON 形式のデータを解析し、1分ごとのウィンドウに分割し、ウィンドウごとに件数をカウントし、BigQuery に書き込んでいます。
    # "beam.io.ReadFromText" でテキストファイルを読み込み、 "beam.Map" で "parse_json" 関数を適用して JSON データを解析し、"beam.WindowInto" でウィンドウ処理を行い、"beam.CombineGlobally" で集計処理を行い、"beam.io.WriteToBigQuery" で BigQuery に書き込んでいます。
    # "p | 'ReadFromGCS' >>..." という記法は、パイプラインに処理を追加する際に使われます。


    # 5.7. ログ設定とパイプライン実行
    logging.getLogger().setLevel(logging.INFO)  # ログレベルを設定
    logging.info("Building pipeline...")  # ログ出力
    p.run()  # パイプラインを実行

    # 最後に、ログの設定を行い、パイプラインを実行しています。
    # "p.run()" を実行することで、定義したパイプラインが実際に動作し、データ処理が行われます。


# 6. メイン処理の実行
if __name__ == '__main__':
  run()

# この部分は、プログラムが直接実行された場合に "run" 関数を呼び出すためのものです。
# Python では、スクリプトが直接実行された場合と、別のスクリプトからモジュールとして読み込まれた場合で、
# 異なる処理を行うことができます。
# "if __name__ == '__main__':" を使うことで、スクリプトが直接実行された場合のみ "run" 関数が実行されるようになります。