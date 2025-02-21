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

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * CommonLogクラスは、Webサーバーのイベントログを表すためのクラスです。
 * JSON形式のログデータを解析して、このクラスのオブジェクトに変換することで、
 * Apache Beamのパイプラインでログデータを扱いやすくします。
 * 
 * @DefaultSchema(JavaFieldSchema.class) アノテーションは、
 * このクラスをBeam Schemaとして使用することを指定しています。
 * これにより、ログデータをRowオブジェクトとして扱うことができるようになり、
 * SQL変換などの機能を利用できるようになります。
 */
@DefaultSchema(JavaFieldSchema.class)
public class CommonLog {
    // ユーザーID
    String user_id;
    // IPアドレス
    String ip;
    // 緯度 (null許容)
    @javax.annotation.Nullable Double lat;
    // 経度 (null許容)
    @javax.annotation.Nullable Double lng;
    // タイムスタンプ (イベント発生時刻)
    String timestamp;
    // HTTPリクエスト
    String http_request;
    // ユーザーエージェント
    String user_agent;
    // HTTPレスポンスコード
    Long http_response;
    // レスポンスのバイト数
    Long num_bytes;
}