---
name: extract-datagrid-candidates
description: 指定モジュールのファイル一覧から DataGrid get/put の候補表を出す（YAML は出さない）
agent: ask
argument-hint: モジュール名。ファイル一覧があれば貼る。例: logic
---

役割: System の静的解析アシスタント。
目的: 指定 Maven モジュールから DataGrid キャッシュの get / put 候補表を出す。YAML はまだ出さない。
Kafka トピックは表に載せない。

先にファイル一覧（`/extract-datagrid-files` の結果）がチャットにあればそれを使う。
無い場合は、モジュール 1 つに対して Infinispan / DataGrid を含むファイルを列挙してから表にする。
パッケージ `jp.co.sample.system` はプレフィックス（サブパッケージを含む）。直下のクラスだけを対象にしない。

今回の範囲はユーザー入力を使う。モジュール名が無ければ質問してから進む。

- Maven モジュール（必須、1つ）
- フロー名は表のメモに残してよいが、ソースに無いならフィルタに使わない
- 対象操作: DataGrid get / put のみ

表形式:

| 方向 | キャッシュ名または設定キー | routeId またはクラス | メソッドまたは API | ファイル | confidence |

方向は `get` または `put`（remove / clear はメモに残し、表の方向は get/put に寄せる）。

制約:

- 創作禁止。ソースに無いキャッシュ名は書かない
- 解決できない値は「未解決: 設定キー名」と書く
- 行番号は必須にしない
- 1件で「対象クラスはこれだけ」と閉じない。同じモジュール内の他ファイルも当たったか書け
- `system-*` 全体を一度に対象にしない。モジュールごとに表を出し、人がマージする
- Kafka の consume / produce はこの表に書かない
- 秘密情報は出さない
- YAML・OpenMetadata の API JSON は出さない
