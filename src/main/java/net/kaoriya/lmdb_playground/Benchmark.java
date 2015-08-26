package net.kaoriya.lmdb_playground;

import java.util.Random;
import java.util.function.Function;

import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.DirectBuffer;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.Transaction;

import static net.kaoriya.lmdb_playground.LongestPrefixMatch.match;
import static net.kaoriya.lmdb_playground.LMDBUtils.*;
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
            try (Transaction tx = env.createTransaction(false)) {
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

    public Result runTest0(Params p) {
        Result r = new Result("no-suffix, no-query for control");
        Random rand = new Random();
        r.start(p.intervalNano);
        while (r.isContinue()) {
            int i = rand.nextInt(this.keys.length);
            String k = this.keys[i];
            r.countQuery(i < this.keyCount);
        }
        r.stop();
        return r;
    }

    public Result runTest1(Params p) {
        Result r = new Result("no-suffix, no-tx");
        Random rand = new Random();
        r.start(p.intervalNano);
        while (r.isContinue()) {
            String k = this.keys[rand.nextInt(this.keys.length)];
            Entry entry = LongestPrefixMatch.match(p.env, p.db, k);
            r.countQuery(entry != null);
        }
        r.stop();
        return r;
    }

    public Result runTest2(Params p) {
        Result r = new Result("no-suffix, with-tx");
        Random rand = new Random();
        r.start(p.intervalNano);
        try (Transaction tx = p.env.createReadTransaction()) {
            while (r.isContinue()) {
                String k = this.keys[rand.nextInt(this.keys.length)];
                Entry entry = LongestPrefixMatch.match(tx, p.db, k);
                r.countQuery(entry != null);
            }
        }
        r.stop();
        return r;
    }

    public Result runTest3(Params p) {
        Result r = new Result("exact-match, no-tx");
        Random rand = new Random();
        r.start(p.intervalNano);
        while (r.isContinue()) {
            String k = this.keys[rand.nextInt(this.keys.length)];
            Entry entry = LongestPrefixMatch.exactMatch(p.env, p.db, k);
            r.countQuery(entry != null);
        }
        r.stop();
        return r;
    }

    public Result runTest4(Params p) {
        Result r = new Result("exact-match, with-tx");
        Random rand = new Random();
        r.start(p.intervalNano);
        try (Transaction tx = p.env.createReadTransaction()) {
            while (r.isContinue()) {
                String k = this.keys[rand.nextInt(this.keys.length)];
                Entry entry = LongestPrefixMatch.exactMatch(tx, p.db, k);
                r.countQuery(entry != null);
            }
        }
        r.stop();
        return r;
    }

    public Result runTest20(Params p) {
        Result r = new Result("get-exact, with-tx");
        Random rand = new Random();
        r.start(p.intervalNano);
        while (r.isContinue()) {
            String k = this.keys[rand.nextInt(this.keys.length)];
            byte[] v = p.db.get(bytes(k));
            r.countQuery(v != null);
        }
        r.stop();
        return r;
    }

    public Result runTest21(Params p) {
        Result r = new Result("get-exact, with-tx");
        Random rand = new Random();
        r.start(p.intervalNano);
        try (Transaction tx = p.env.createReadTransaction()) {
            while (r.isContinue()) {
                String k = this.keys[rand.nextInt(this.keys.length)];
                byte[] v = p.db.get(tx, bytes(k));
                r.countQuery(v != null);
            }
        }
        r.stop();
        return r;
    }

    public Result runTest22(Params p) {
        Result r = new Result("get-less-copy, with-tx");
        Random rand = new Random();
        DirectBuffer kbuf = new DirectBuffer();
        DirectBuffer vbuf = new DirectBuffer();
        r.start(p.intervalNano);
        while (r.isContinue()) {
            String k = this.keys[rand.nextInt(this.keys.length)];
            kbuf.wrap(bytes(k));
            // XXX: Doesn't work for Windows.
            int rc = p.db.get(kbuf, vbuf);
            r.countQuery(rc != MDB_NOTFOUND);
        }
        r.stop();
        return r;
    }

    public Result runTest10(Params p) {
        Result r = new Result("with-suffix, no-query for control");
        Random rand = new Random();
        r.start(p.intervalNano);
        while (r.isContinue()) {
            int i = rand.nextInt(this.keys.length);
            String k = this.keys[i];
            k += this.suffixGen.generate();
            r.countQuery(i < this.keyCount);
        }
        r.stop();
        return r;
    }

    public Result runTest11(Params p) {
        Result r = new Result("with-suffix, no-tx");
        Random rand = new Random();
        r.start(p.intervalNano);
        while (r.isContinue()) {
            String k = this.keys[rand.nextInt(this.keys.length)];
            k += this.suffixGen.generate();
            Entry entry = LongestPrefixMatch.match(p.env, p.db, k);
            r.countQuery(entry != null);
        }
        r.stop();
        return r;
    }

    public Result runTest12(Params p) {
        Result r = new Result("with-suffix, with-tx");
        Random rand = new Random();
        r.start(p.intervalNano);
        try (Transaction tx = p.env.createReadTransaction()) {
            while (r.isContinue()) {
                String k = this.keys[rand.nextInt(this.keys.length)];
                k += this.suffixGen.generate();
                Entry entry = LongestPrefixMatch.match(tx, p.db, k);
                r.countQuery(entry != null);
            }
        }
        r.stop();
        return r;
    }

    public void measureBenchmark() throws Exception {
        runNewEnv(this.dir, false, (env, db) -> {
            Runner runner = new Runner(BENCHMARK_DURATION, env, db);

            //runner.run(this::runTest0);
            runner.run(this::runTest1);
            runner.run(this::runTest2);

            // get-exact is not used. slow, redundant.
            //runner.run(this::runTest3);
            //runner.run(this::runTest4);

            // with-suffix commented, not now.
            //runner.run(this::runTest10);
            //runner.run(this::runTest11);
            //runner.run(this::runTest12);

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
