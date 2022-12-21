package net.kaoriya.lmdb_playground;

import java.util.Random;
import java.util.function.Function;
import java.util.function.BiFunction;

import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.DirectBuffer;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.Transaction;

import static net.kaoriya.lmdb_playground.LongestPrefixMatch.match;
import static net.kaoriya.lmdb_playground.LMDBUtils.runNewEnv;
import static org.fusesource.lmdbjni.Constants.bytes;
import static org.fusesource.lmdbjni.Constants.string;

/**
 * Benchmark of LongestPrefixMatch.match
 */
public class Benchmark {

    public final static long SECOND = 1000_000_000L;
    public final static long BENCHMARK_DURATION = 5 * SECOND;
    public final static int MDB_NOTFOUND = -30798;

    private String dir;
    private double hitRate;
    private int keyCount;
    private int keyLen = 128;
    private int valLen = 128;
    private int keySuffixLen = 16;

    private String[] keys;
    private Generator suffixGen;

    public Benchmark(String dir, double hitRate, int keyCount) {
        this.dir = dir;
        this.hitRate = hitRate;
        this.keyCount = keyCount;
    }

    public void setup() throws Exception {
        int allKeyCount = (int)((double)this.keyCount / this.hitRate);
        this.keys = new String[allKeyCount];

        Generator keyGen = new Generator(this.keyLen / 2);
        Generator valGen = new Generator(this.valLen);

        runNewEnv(this.dir, true, (env, db) -> {
            env.setMapSize(536870912); // 512MB
            try (Transaction tx = env.createWriteTransaction()) {
                for (int i = 0; i < this.keyCount; ++i) {
                    String k = keyGen.generate();
                    String v = valGen.generate();
                    this.keys[i] = k;
                    db.put(tx, bytes(k), bytes(v));
                }
                tx.commit();
            }
            //System.out.println("stat=" + env.stat().toString());
        });

        for (int i = this.keyCount; i < allKeyCount; ++i) {
            this.keys[i] = keyGen.generate();
        }

        this.suffixGen = new Generator(this.keySuffixLen);
    }

    private Result runWithoutTx(
            Params p,
            String label,
            BiFunction<Integer, String, Boolean> f)
    {
        Result r = new Result(label);
        Random rand = new Random();
        r.start(p.intervalNano);
        while (r.isContinue()) {
            int i = rand.nextInt(this.keys.length);
            String k = this.keys[i];
            r.countQuery(f.apply(i, k));
        }
        r.stop();
        return r;
    }

    private Result runWithTx(
            Params p,
            String label,
            BiFunction<Transaction, String, Boolean> f)
    {
        Result r = new Result(label);
        Random rand = new Random();
        r.start(p.intervalNano);
        try (Transaction tx = p.env.createReadTransaction()) {
            while (r.isContinue()) {
                int i = rand.nextInt(this.keys.length);
                String k = this.keys[i];
                r.countQuery(f.apply(tx, k));
            }
        }
        r.stop();
        return r;
    }

    public Result runTest1(Params p) {
        return runWithoutTx(p, "no-suffix, no-tx", (i, k) -> {
            Entry entry = LongestPrefixMatch.match(p.env, p.db, k);
            return entry != null;
        });
    }

    public Result runTest2(Params p) {
        return runWithTx(p, "no-suffix, with-tx", (tx, k) -> {
            Entry entry = LongestPrefixMatch.match(tx, p.db, k);
            return entry != null;
        });
    }

    public Result runTest20(Params p) {
        return runWithoutTx(p, "get-exact, no-tx", (i, k) -> {
            byte[] v = p.db.get(bytes(k));
            return v != null;
        });
    }

    public Result runTest21(Params p) {
        return runWithTx(p, "get-exact, with-tx", (tx, k) -> {
            byte[] v = p.db.get(tx, bytes(k));
            return v != null;
        });
    }

    public Result runTest22(Params p) {
        DirectBuffer kbuf = new DirectBuffer();
        DirectBuffer vbuf = new DirectBuffer();
        return runWithoutTx(p, "get-less-copy, no-tx", (i, k) -> {
            kbuf.wrap(bytes(k));
            // XXX: Doesn't work for Windows.
            int rc = p.db.get(kbuf, vbuf);
            return rc != MDB_NOTFOUND;
        });
    }

    public Result runTest11(Params p) {
        return runWithoutTx(p, "with-suffix, no-tx", (i, k) -> {
            k += this.suffixGen.generate();
            Entry entry = LongestPrefixMatch.match(p.env, p.db, k);
            return entry != null;
        });
    }

    public Result runTest12(Params p) {
        return runWithTx(p, "with-suffix, with-tx", (tx, k) -> {
            k += this.suffixGen.generate();
            Entry entry = LongestPrefixMatch.match(tx, p.db, k);
            return entry != null;
        });
    }

    public void measureBenchmark() throws Exception {
        runNewEnv(this.dir, false, (env, db) -> {
            Runner runner = new Runner(BENCHMARK_DURATION, env, db);

            // no-suffix
            runner.run(this::runTest1);
            runner.run(this::runTest2);

            // with-suffix
            runner.run(this::runTest11);
            runner.run(this::runTest12);

            // get-exact (for control)
            runner.run(this::runTest20);
            runner.run(this::runTest21);
            // XXX: less-copy doesn't work on Windows.
            //runner.run(this::runTest22);
        });
    }

    public void run() {
        try {
            System.out.println(toString());
            setup();
            measureBenchmark();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String toString() {
        return String.format(
                "HitRate:%.2f KeyCount:%d KeyLen:%d ValLen:%d",
                this.hitRate, this.keyCount, this.keyLen, this.valLen);
    }

    public static class Generator {

        private Random r = new Random();
        private char[] buf;
        private String table = "012345679"
            + "abcdefghijklmnopqrstuvwxyz"
            + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            + "/.@";

        public Generator(int len) {
            this.buf = new char[len];
        }

        public String generate() {
            for (int i = 0; i < this.buf.length; ++i) {
                this.buf[i] = getChar();
            }
            return new String(this.buf, 0, this.buf.length);
        }

        private char getChar() {
            return this.table.charAt(this.r.nextInt(this.table.length()));
        }
    }

    public static class Params {
        public final long intervalNano;
        public final Env env;
        public final Database db;

        public Params(long intervalNano, Env env, Database db) {
            this.intervalNano = intervalNano;
            this.env = env;
            this.db = db;
        }
    }

    public static class Runner {
        public final Params params;

        public Runner(long intervalNano, Env env, Database db) {
            this.params = new Params(intervalNano, env, db);
        }

        public void run(Function<Params, Result> f) {
            Result r = f.apply(this.params);
            System.out.println("  " + r.toString());
        }
    }

    public static class Result {
        public String label;
        public long startAt;
        public long willBeEndAt;
        public long stopAt;
        public long queryCount;
        public long hitCount;

        public Result(String label) {
            this.label = label;
        }

        public void start(long intervalNano) {
            this.startAt = System.nanoTime();
            this.willBeEndAt = this.startAt + intervalNano;
        }

        public void stop() {
            this.stopAt = System.nanoTime();
        }

        public boolean isContinue() {
            return System.nanoTime() < this.willBeEndAt;
        }

        public void countQuery(boolean hit) {
            ++this.queryCount;
            if (hit) {
                ++this.hitCount;
            }
        }

        public String toString() {
            return String.format(
                    "%1$s - QPS:%2$.2f HitRate:%3$.3f",
                    this.label,
                    (double)(this.queryCount) / (this.stopAt - this.startAt) * SECOND,
                    (double)(this.hitCount) / this.queryCount);
        }
    }

    public static void main(String[] args) {
        Benchmark b1 = new Benchmark("tmp/benchmark/001", 0.1, 100000);
        b1.run();
        Benchmark b2 = new Benchmark("tmp/benchmark/002", 0.5, 100000);
        b2.run();
        Benchmark b3 = new Benchmark("tmp/benchmark/003", 0.9, 100000);
        b3.run();
    }
}
