# Lmdb playground

Test implementation of **longest prefix match** using lmdbjni.

## lmdb JNI

*   Github: https://github.com/deephacks/lmdbjni
*   Javadoc: http://deephacks.org/lmdbjni/apidocs/index.html

## Benchamrk

### How to run benchmark

    $ gradle run

### Benchmark results

#### 2015/09/09 version

```
HitRate:0.10 KeyCount:100000 KeyLen:128 ValLen:128
  no-suffix, no-tx - QPS:221559.29 HitRate:0.100
  no-suffix, with-tx - QPS:250753.34 HitRate:0.100
  with-suffix, no-tx - QPS:203924.75 HitRate:0.100
  with-suffix, with-tx - QPS:246871.87 HitRate:0.100
  get-exact, no-tx - QPS:734347.91 HitRate:0.100
  get-exact, with-tx - QPS:930207.52 HitRate:0.100
HitRate:0.50 KeyCount:100000 KeyLen:128 ValLen:128
  no-suffix, no-tx - QPS:246747.14 HitRate:0.499
  no-suffix, with-tx - QPS:283585.96 HitRate:0.501
  with-suffix, no-tx - QPS:202871.14 HitRate:0.499
  with-suffix, with-tx - QPS:222302.44 HitRate:0.500
  get-exact, no-tx - QPS:785039.09 HitRate:0.500
  get-exact, with-tx - QPS:960080.41 HitRate:0.500
HitRate:0.90 KeyCount:100000 KeyLen:128 ValLen:128
  no-suffix, no-tx - QPS:259878.37 HitRate:0.900
  no-suffix, with-tx - QPS:250179.26 HitRate:0.900
  with-suffix, no-tx - QPS:204256.73 HitRate:0.899
  with-suffix, with-tx - QPS:218295.46 HitRate:0.900
  get-exact, no-tx - QPS:756443.89 HitRate:0.900
  get-exact, with-tx - QPS:965200.35 HitRate:0.900
```


## How to use `LongestPrefixMatch`

Copy one or two `*.java` files to your project:

*   [LongestPrefixMatch.java](https://raw.githubusercontent.com/koron/java-lmdb-playground/master/src/main/java/net/kaoriya/lmdb_playground/LongestPrefixMatch.java)
*   (OPTION) [LMDBUtils.java](https://raw.githubusercontent.com/koron/java-lmdb-playground/master/src/main/java/net/kaoriya/lmdb_playground/LMDBUtils.java)

(OPTION) Copy [`*.jar` files](https://github.com/koron/java-lmdb-playground/tree/master/lib) to your project.

Then you can use `LongestPrefixMatch#match` for the purpose.

```java
LongestPrefixMatch.match(env, db, "foobar");
```

To obtain `env` and `db` easily, you could use `LMDBUtils#runNewEnv` too.
Please check below example:

```java
LMDBUtils.runNewEnv("/var/db/my_prefix_data", false, (env, db) -> {
  LongestPrefixMatch.match(env, db, "foobar");
});
```


## License

This project and its all contents are distributed under Apache License 2.0
See LICENSE for details.
