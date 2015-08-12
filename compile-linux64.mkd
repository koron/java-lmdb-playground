# How to compile own lmdbjni for Linux

## 手順

**初回のビルド**

    $ git clone git@github.com:deephacks/lmdbjni.git
    $ cd lmdbjni
    $ mvn -P linux64 package

Output is `lmdbjni/lmdbjni-linux64/target/lmdbjni-linux64-0.4.3-SNAPSHOT.jar`

**キーサイズを大きくする**

lmdbjni/src/test/java/org/fusesource/lmdbjni/EnvTest.java の 152 行目付近の
`511L` を `1023L` に書き換える。
キーの最大長として 1023 以外を使う場合は適宜読み替える。

変更前:

```java
assertThat(env.getMaxKeySize(), is(511L));
```

変更後:

```java
assertThat(env.getMaxKeySize(), is(1023L));
```

その後、以下のコマンドでビルドし直す。

    $ CFLAGS="-DMDB_MAXKEYSIZE=1023" mvn -P linux64 clean package
