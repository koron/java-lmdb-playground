package net.kaoriya.lmdb_playground;

import java.util.Random;

import org.fusesource.lmdbjni.Transaction;

import static net.kaoriya.lmdb_playground.LongestPrefixMatch.match;
import static net.kaoriya.lmdb_playground.LMDBUtils.*;
import static org.fusesource.lmdbjni.Constants.bytes;
import static org.fusesource.lmdbjni.Constants.string;

/**
 * Benchmark of LongestPrefixMatch.match
 */
public class Benchmark {

    private String dir;
    private double hitRate;
    private int keyCount;
    private int keyLen = 128;
    private int valLen = 128;

    private String[] keys;

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
    }

    public void measureBenchmark() throws Exception {
        runNewEnv(this.dir, false, (env, db) -> {
        });
    }

    public void run() {
        try {
            System.out.println("setup()");
            setup();
            System.out.println("measureBenchmark()");
            measureBenchmark();
            System.out.println("DONE");
            measureBenchmark();
        } catch (Exception e) {
            e.printStackTrace();
        }
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

    public static void main(String[] args) {
        Benchmark b = new Benchmark("tmp/benchmark/001", 0.1, 100000);
        b.run();
    }

}
