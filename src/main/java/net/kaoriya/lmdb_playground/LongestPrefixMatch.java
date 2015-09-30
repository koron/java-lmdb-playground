package net.kaoriya.lmdb_playground;

import java.util.Arrays;

import org.fusesource.lmdbjni.Cursor;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.SeekOp;
import org.fusesource.lmdbjni.Transaction;

import static org.fusesource.lmdbjni.Constants.bytes;
import static org.fusesource.lmdbjni.Constants.string;

/**
 * Longest prefix match implementation using lmdb-jni.
 */
public class LongestPrefixMatch {

    /**
     * Perform longest prefix match.
     *
     * Make a match with implicit temporal transaction.
     *
     * @param env instance of Env
     * @param db instance of Database
     * @param s query string
     */
    public static Entry match(Env env, Database db, String s) {
        try (Transaction tx = env.createReadTransaction()) {
            return match(tx, db, s);
        }
    }

    /**
     * Perform longest prefix match with a transaction.
     *
     * Explicit trancation version for speed.
     *
     * @param tx instance of Transaction
     * @param db instance of Database
     * @param s query string
     */
    public static Entry match(Transaction tx, Database db, String s) {
        return match1(tx, db, s);
    }

    public static Entry match1(Transaction tx, Database db, String s) {
        if (s == null || s.length() == 0) {
            return null;
        }
        try (Cursor c = db.openCursor(tx)) {
            Entry found = null;
            for (int i = 1, l = s.length(); i <= l; ++i) {
                byte[] queryBytes = bytes(s.substring(0, i));
                Entry e = c.seek(SeekOp.RANGE, queryBytes);
                if (e == null) {
                    break;
                }
                /*
                byte[] keyBytes = e.getKey();
                if (Arrays.equals(queryBytes, keyBytes)) {
                    found = e;
                    continue;
                }
                */
                String keyString = string(e.getKey());
                int n = countPrefixMatch(s, keyString);
                if (n < i) {
                    break;
                } else if (n >= i) {
                    i = n;
                    if (n == keyString.length()) {
                        found = e;
                    }
                }
            }
            return found;
        }
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

    public static Entry match2(Transaction tx, Database db, String s) {
        if (s == null || s.length() == 0) {
            return null;
        }
        byte[] targetBytes = bytes(s);
        try (Cursor c = db.openCursor(tx)) {
            Entry found = null;
            for (int i = 1, l = targetBytes.length; i <= l; ++i) {
                byte[] queryBytes = Arrays.copyOf(targetBytes, i);
                Entry e = c.seek(SeekOp.RANGE, queryBytes);
                if (e == null) {
                    break;
                }
                /*
                byte[] keyBytes = e.getKey();
                if (Arrays.equals(queryBytes, keyBytes)) {
                    found = e;
                    continue;
                }
                */
                int n = countPrefixMatch(targetBytes, keyBytes);
                if (n < i) {
                    break;
                } else if (n >= i) {
                    i = n;
                    if (n == keyBytes.length) {
                        found = e;
                    }
                }
            }
            return found;
        }
    }

    static int countPrefixMatch(byte[] s, byte[] t) {
        int max = Math.min(s.length, t.length);
        int i;
        for (i = 0; i < max; ++i) {
            if (s[i] != t[i]) {
                break;
            }
        }
        return i;
    }

}
