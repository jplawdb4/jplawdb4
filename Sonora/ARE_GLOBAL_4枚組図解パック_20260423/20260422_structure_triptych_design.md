# ARE GLOBAL 3枚組図解 設計メモ

## 目的

既存の `全体ストラクチャー1枚図` では、構造の輪郭は見える一方で、`主体ごとの接続関係` と `証拠・未解明 frontier` の読み分けがやや詰まりやすい。  
そのため今回は、情報を次の3枚に分解して、`全体俯瞰 -> 主体関係 -> 証拠と次アクション` の順に読める triptych に再構成する。

## 3枚の役割

### Image 1

- タイトル案: `ARE GLOBAL クロスボーダー構造 全体俯瞰`
- 役割:
  - もっとも上位の構造を一目で把握させる
  - `Singapore layer`, `Japan mirror layer`, `Address / administration hub`, `Unresolved ownership chain` を整理する
  - OMORI と ESAKA を対比しつつ、cluster 背景も見せる
- 情報密度:
  - 中程度
  - ボックス数は多すぎず、まず全体像を掴ませる

### Image 2

- タイトル案: `OMORI / ESAKA 主体別レイヤー・接続関係図`
- 役割:
  - 6主体を個別に読み解く
  - `OMORI SG -> OMORI JP GK + 一般社団法人`, `ESAKA SG -> ESAKA JP GK + 一般社団法人` の mirror-like pairing を主題化する
  - 住所ハブ、status、JCN / UEN、閉鎖 / active、QII filing の差を見せる
- 情報密度:
  - 高め
  - ただし evidence の長文引用は減らし、主体ノード中心にする

### Image 3

- タイトル案: `公開証拠・確度・未解明 frontier`
- 役割:
  - どの証拠がどの接続を支えているかを整理する
  - `fact`, `high-likelihood inference`, `unresolved` を主役にする
  - DD / CFC 上の次の取得資料を明示する
- 情報密度:
  - 高い
  - 実務家向けの evidence matrix 風にする

## 3枚の読み順

1. `Image 1` で全体像を掴む
2. `Image 2` で各主体の配置と接続を確認する
3. `Image 3` で何が立証済みで、何が未立証かを理解する

## 共通ルール

- 横長、A3横相当
- 日本語テキスト主体
- 余計なイラストやアイコンは使わない
- 背景は明るいオフホワイト
- `Singapore = blue`, `Japan = amber / warm gray`, `Evidence = steel blue`, `Unresolved = muted red`
- `solid = fact`, `dashed = inference`, `red dotted = unresolved`
- 強い断定は避け、`vehicle-like`, `mirror-like`, `not yet proven` のニュアンスを残す

## 保存物

- 各画像について:
  - source memo
  - imagegen prompt txt
  - generated image png
- さらに:
  - 3枚組 index.html
  - CHANGE_LOG.md

## 追補: Image 4

- 2026-04-23 の追加図は `3枚の補足` ではなく `architecture spine` として置く
- タイトル案: `OMORI / ESAKA / ARE GLOBAL 6層ストラクチャー図`
- 役割:
  - `個別主体` や `証拠frontier` ではなく `全体がどの層で積まれているか` を見せる
  - `Singapore vehicle`, `Japan mirror pair`, `administration hub`, `transaction / regulatory trace`, `cluster`, `ownership frontier` を1枚で縦積みする
- 位置づけ:
  - Image 1-3 を読んだあとに、`結局この全体はどういう architecture なのか` を一目で再固定する図
