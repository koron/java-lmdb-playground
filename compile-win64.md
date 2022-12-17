# How to compile own lmdbjni for Win64

## 必要なもの

*   Visual Studio 2010
*   MinGW/GCC for Win64 ([tdm-gcc](http://tdm-gcc.tdragon.net/) など)
    *   gcc
    *   ar
    *   make
    *   sh (bash)
*   maven
*   git

## 事前準備

コマンドラインから Visual Studio と GCC を利用できるようにパスなどを設定してお
く。

## 手順

**初回のビルド**

```
$ git clone git@github.com:deephacks/lmdb.git
$ cd lmdb/lmdb/src/main/native
$ make clean
$ cd ../../../..
$ mvn -P win64 package
$ cd ..

$ git clone git@github.com:deephacks/lmdbjni.git
$ cd lmdbjni
$ mvn -P win64 package
$ rm -r lmdbjni-win64/target/lmdb/META-INF
$ find target -name '*.dll' | xargs rm
$ unzip -d lmdbjni-win64/target/lmdb ../lmdb/lmdb-win64/target/lmdb-win64-0.1.4-SNAPSHOT.jar
$ mvn -P win64 package
$ cd ..
```

Output is `lmdbjni/lmdbjni-win64/target/lmdbjni-win64-0.4.3-SNAPSHOT.jar`

**lmdbを更新した場合**

より少ない手順で行う方法も考えられますが、
万が一の更新漏れを防ぐことを優先しています。

```
$ cd lmdb/lmdb/src/main/native
$ make clean
$ cd ../../../..
$ mvn -P win64 package
$ cd ../lmdbjni
$ rm -r lmdbjni-win64/target/lmdb/META-INF
$ find lmdbjni-win64/target -name '*.dll' | xargs rm
$ unzip -d lmdbjni-win64/target/lmdb ../lmdb/lmdb-win64/target/lmdb-win64-0.1.4-SNAPSHOT.jar
$ mvn -P win64 package
$ cd ..
```

Output is `lmdbjni/lmdbjni-win64/target/lmdbjni-win64-0.4.3-SNAPSHOT.jar`

## lmdbの修正の一例

**キーサイズを大きくする**

lmdb/lmdb-win64/pom.xml の 42行目付近の `<argument>` 要素に `-DMDB_MAXKEYSIZE=1023` を追加する。

変更前:

    <argument>XCFLAGS=-fno-stack-check -fno-stack-protector -mno-stack-arg-probe -ffunction-sections</argument>

変更後:

    <argument>XCFLAGS=-fno-stack-check -fno-stack-protector -mno-stack-arg-probe -ffunction-sections -DMDB_MAXKEYSIZE=1023</argument>

## 留意事項

ComparatorTest が失敗している関係で、それ以降のテストが実行されません。
