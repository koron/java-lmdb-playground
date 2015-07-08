package net.kaoriya.lmdb_playground;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import org.fusesource.lmdbjni.Cursor;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.EntryIterator;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.GetOp;
import org.fusesource.lmdbjni.SeekOp;
import org.fusesource.lmdbjni.Transaction;

import static org.fusesource.lmdbjni.Constants.bytes;
import static org.fusesource.lmdbjni.Constants.string;

import static net.kaoriya.lmdb_playground.LMDBUtils.*;

public class LongestPrefixMatch {

    public static void main(String[] args) {
        lpm01();
    }

    static void lpm01() {
        runNewEnv("tmp/lpm01", true, (env, db) -> {
            putKeys(db, new String[]{
                "aa", "ab", "abc",
                "ba", "bb", "bc",
                "ca", "cbd", "cc",
            });
            // FOUND: prefix=abc key=abc
            // FOUND: prefix=bbc key=bb
            // FOUND: prefix=cbc key=cbd
            test(env, db, "abc");
            test(env, db, "bbc");
            test(env, db, "cbc");
        });
    }

    static void test(Env env, Database db, String s) {
        Entry e = match(env, db, s);
        if (e != null) {
            System.out.printf("FOUND: prefix=%s key=%s", s,
                    string(e.getKey()));
            System.out.println();
        } else {
            System.out.printf("NOT_FOUND: prefix=%s", s);
        }
    }

    static Entry match(Env env, Database db, String s) {
        Transaction tx = env.createTransaction(true);
        try (Cursor c = db.openCursor(tx)) {
            Entry e = c.seek(SeekOp.RANGE, bytes(s));
            if (e != null && hasKeyPrefix(e, s)) {
                return e;
            }
            int longestLen = countPrefixMatch(string(e.getKey()), s);
            Entry longestEntry = longestLen > 0 ? e : null;
            while (true) {
                e = c.get(GetOp.PREV);
                if (e == null) {
                    break;
                }
                String k = string(e.getKey());
                if (s.startsWith(k)) {
                    return e;
                }
                int l = countPrefixMatch(k, s);
                if (l == 0) {
                    break;
                } else if (longestEntry == null || l >= longestLen) {
                    longestLen = l;
                    longestEntry = e;
                }
            }
            return longestEntry;
        } finally {
            tx.reset();
        }
    }

    static boolean hasKeyPrefix(Entry e, String prefix) {
        return string(e.getKey()).startsWith(prefix);
    }

    static int countPrefixMatch(String s, String t) {
        int max = Math.min(s.length(), t.length());
        int i;
        for (i = 0; i < max; ++i) {
            if (s.charAt(i) != t.charAt(i)) {
                break;
            }
        }
        return i;
    }

}
