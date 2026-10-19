---
name: extract-datagrid-yaml
description: 確度 high の DataGrid 候補だけ、中間 YAML にする（Kafka は混ぜない）
agent: ask
argument-hint: フロー名（出力ラベル）と候補表。モジュール名があれば付ける
---

役割: System の静的解析アシスタント。
目的: DataGrid キャッシュの候補表を、OpenMetadata 投入用の中間 YAML にする。
スキーマの正: `#file:lineage/datagrid-cache.example.yaml`（ファイルが無ければ `copilot-instructions.md` の DataGrid 骨格に従う）

候補表がチャットにある場合は、その confidence が high のものだけ使う。
候補表が無い、またはモジュール横断で未処理のモジュールがある場合は YAML を出さず、先に `/extract-datagrid-files` または候補表を出せ。

今回の範囲はユーザー入力を使う。

- フロー名（`metadata.name` と出力ファイル名。ソース検索には使わない）
- 対象操作: DataGrid get / put のみ。Kafka トピックは YAML に入れない

手順:

1. 候補表の high だけ YAML にする
2. Pipeline ノード名は `routeId` 優先
3. Table ノード名は実行時のキャッシュ名。クラス名をキャッシュ名にしない
4. 変数のままのキャッシュ名は YAML に入れず、最後にコメントで列挙する
5. put は `pipeline -> table`、get は `table -> pipeline`

制約:

- 出力は YAML のみ。前後の解説は付けない（コメントでの未解決列挙は可）
- `metadata` / `services` / `nodes` / `edges` 以外のキーを増やさない
- services は既存の `process` / `datagrid` を再利用する（`kafka` は出さない）
- `type` は `pipeline` または `table` のみ
- table には `service: datagrid`、`database: caches`、`schema: default` を付ける
- `nodes.id` と `edges.from` / `to` を一致させる
- 根拠を各 node の `description` に `path: ... symbol: ...` で残す
- 接続情報・認証はダミーのまま。`saslMechanism` など Kafka 用フィールドを DataGrid に載せない
- ソースに無い名前は作らない
- ファイルに保存する場合のパスは `lineage/<flow>-datagrid.yaml`。`*.example.yaml` は上書きしない
