package net.kaoriya.lmdb_playground;

import java.util.Random;

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

    public Result runTest0(
            long intervalNano,
            Env env,
            Database db)
    {
        Result r = new Result("no-suffix, no-query for control");
        Random rand = new Random();
        r.start(intervalNano);
        while (r.isContinue()) {
            int i = rand.nextInt(this.keys.length);
            String k = this.keys[i];
            r.countQuery(i < this.keyCount);
        }
        r.stop();
        return r;
    }

    public Result runTest1(
            long intervalNano,
            Env env,
            Database db)
    {
        Result r = new Result("no-suffix, no-tx");
        Random rand = new Random();
        r.start(intervalNano);
        while (r.isContinue()) {
            String k = this.keys[rand.nextInt(this.keys.length)];
            Entry entry = LongestPrefixMatch.match(env, db, k);
            r.countQuery(entry != null);
        }
        r.stop();
        return r;
    }

    public Result runTest2(
            long intervalNano,
            Env env,
            Database db)
    {
        Result r = new Result("no-suffix, with-tx");
        Random rand = new Random();
        r.start(intervalNano);
        try (Transaction tx = env.createReadTransaction()) {
            while (r.isContinue()) {
                String k = this.keys[rand.nextInt(this.keys.length)];
                Entry entry = LongestPrefixMatch.match(tx, db, k);
                r.countQuery(entry != null);
            }
        }
        r.stop();
        return r;
    }

    public Result runTest3(
            long intervalNano,
            Env env,
            Database db)
    {
        Result r = new Result("exact-match, no-tx");
        Random rand = new Random();
        r.start(intervalNano);
        while (r.isContinue()) {
            String k = this.keys[rand.nextInt(this.keys.length)];
            Entry entry = LongestPrefixMatch.exactMatch(env, db, k);
            r.countQuery(entry != null);
        }
        r.stop();
        return r;
    }

    public Result runTest4(
            long intervalNano,
            Env env,
            Database db)
    {
        Result r = new Result("exact-match, with-tx");
        Random rand = new Random();
        r.start(intervalNano);
        try (Transaction tx = env.createReadTransaction()) {
            while (r.isContinue()) {
                String k = this.keys[rand.nextInt(this.keys.length)];
                Entry entry = LongestPrefixMatch.exactMatch(tx, db, k);
                r.countQuery(entry != null);
            }
        }
        r.stop();
        return r;
    }

    public Result runTest20(
            long intervalNano,
            Env env,
            Database db)
    {
        Result r = new Result("get-exact, with-tx");
        Random rand = new Random();
        r.start(intervalNano);
        while (r.isContinue()) {
            String k = this.keys[rand.nextInt(this.keys.length)];
            byte[] v = db.get(bytes(k));
            r.countQuery(v != null);
        }
        r.stop();
        return r;
    }

    public Result runTest21(
            long intervalNano,
            Env env,
            Database db)
    {
        Result r = new Result("get-exact, with-tx");
        Random rand = new Random();
        r.start(intervalNano);
        try (Transaction tx = env.createReadTransaction()) {
            while (r.isContinue()) {
                String k = this.keys[rand.nextInt(this.keys.length)];
                byte[] v = db.get(tx, bytes(k));
                r.countQuery(v != null);
            }
        }
        r.stop();
        return r;
    }

    public Result runTest22(
            long intervalNano,
            Env env,
            Database db)
    {
        Result r = new Result("get-less-copy, with-tx");
        Random rand = new Random();
        DirectBuffer kbuf = new DirectBuffer();
        DirectBuffer vbuf = new DirectBuffer();
        r.start(intervalNano);
        while (r.isContinue()) {
            String k = this.keys[rand.nextInt(this.keys.length)];
            kbuf.wrap(bytes(k));
            // XXX: Doesn't work for Windows.
            int rc = db.get(kbuf, vbuf);
            r.countQuery(rc != MDB_NOTFOUND);
        }
        r.stop();
        return r;
    }

    public Result runTest10(
            long intervalNano,
            Env env,
            Database db)
    {
        Result r = new Result("with-suffix, no-query for control");
        Random rand = new Random();
        r.start(intervalNano);
        while (r.isContinue()) {
            int i = rand.nextInt(this.keys.length);
            String k = this.keys[i];
            k += this.suffixGen.generate();
            r.countQuery(i < this.keyCount);
        }
        r.stop();
        return r;
    }

    public Result runTest11(
            long intervalNano,
            Env env,
            Database db)
    {
        Result r = new Result("with-suffix, no-tx");
        Random rand = new Random();
        r.start(intervalNano);
        while (r.isContinue()) {
            String k = this.keys[rand.nextInt(this.keys.length)];
            k += this.suffixGen.generate();
            Entry entry = LongestPrefixMatch.match(env, db, k);
            r.countQuery(entry != null);
        }
        r.stop();
        return r;
    }

    public Result runTest12(
            long intervalNano,
            Env env,
            Database db)
    {
        Result r = new Result("with-suffix, with-tx");
        Random rand = new Random();
        r.start(intervalNano);
        try (Transaction tx = env.createReadTransaction()) {
            while (r.isContinue()) {
                String k = this.keys[rand.nextInt(this.keys.length)];
                k += this.suffixGen.generate();
                Entry entry = LongestPrefixMatch.match(tx, db, k);
                r.countQuery(entry != null);
            }
        }
        r.stop();
        return r;
    }

    public void measureBenchmark() throws Exception {
        runNewEnv(this.dir, false, (env, db) -> {
            //Result r0 = runTest0(BENCHMARK_DURATION, env, db);
            //System.out.println("  " + r0.toString());

            Result r1 = runTest1(BENCHMARK_DURATION, env, db);
            System.out.println("  " + r1.toString());
            Result r2 = runTest2(BENCHMARK_DURATION, env, db);
            System.out.println("  " + r2.toString());

            //Result r3 = runTest3(BENCHMARK_DURATION, env, db);
            //System.out.println("  " + r3.toString());
            //Result r4 = runTest4(BENCHMARK_DURATION, env, db);
            //System.out.println("  " + r4.toString());

            //Result r10 = runTest10(BENCHMARK_DURATION, env, db);
            //System.out.println("  " + r10.toString());
            //Result r11 = runTest11(BENCHMARK_DURATION, env, db);
            //System.out.println("  " + r11.toString());
            //Result r12 = runTest12(BENCHMARK_DURATION, env, db);
            //System.out.println("  " + r12.toString());

            Result r20 = runTest20(BENCHMARK_DURATION, env, db);
            System.out.println("  " + r20.toString());
            Result r21 = runTest21(BENCHMARK_DURATION, env, db);
            System.out.println("  " + r21.toString());

            //Result r22 = runTest22(BENCHMARK_DURATION, env, db);
            //System.out.println("  " + r22.toString());
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
