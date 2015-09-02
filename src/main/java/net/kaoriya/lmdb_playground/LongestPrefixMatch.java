package net.kaoriya.lmdb_playground;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

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

    public static Entry match(Env env, Database db, String s) {
        Transaction tx = env.createReadTransaction();
        try {
            return match(tx, db, s);
        } finally {
            tx.reset();
            tx.close();
        }
    }

    public static Entry match(Transaction tx, Database db, String s) {
        return match2(tx, db, s);
    }

    private static Entry match1(Transaction tx, Database db, String s) {
        try (Cursor c = db.openCursor(tx)) {
            Entry e = c.seek(SeekOp.RANGE, bytes(s));
            if (e == null || hasKeyPrefix(e, s)) {
                return e;
            }
            // scan the key which is prefix of query string.
            boolean subMatch = countPrefixMatch(string(e.getKey()), s) > 0;
            while (true) {
                e = c.get(GetOp.PREV);
                if (e == null) {
                    break;
                }
                String k = string(e.getKey());
                if (s.startsWith(k)) {
                    return e;
                }
                boolean newSubMatch = countPrefixMatch(k, s) > 0;
                if (!newSubMatch && subMatch) {
                    return null;
                }
                subMatch |= newSubMatch;
            }
            return null;
        }
    }

    private static Entry match2(Transaction tx, Database db, String s) {
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
                byte[] keyBytes = e.getKey();
                if (Arrays.equals(queryBytes, keyBytes)) {
                    found = e;
                    continue;
                }
                String keyString = string(e.getKey());
                int n = countPrefixMatch(s, keyString);
                if (n < i) {
                    break;
                } else if (n > i) {
                    i = n;
                    if (n == keyString.length()) {
                        found = e;
                    }
                }
            }
            return found;
        }
    }

    public static Entry exactMatch(Env env, Database db, String s) {
        Transaction tx = env.createReadTransaction();
        try {
            return exactMatch(tx, db, s);
        } finally {
            tx.reset();
            tx.close();
        }
    }

    public static Entry exactMatch(Transaction tx, Database db, String s) {
        try (Cursor c = db.openCursor(tx)) {
            Entry e = c.seek(SeekOp.RANGE, bytes(s));
            if (e == null || hasKeyMatch(e, s)) {
                return e;
            }
            return null;
        }
    }

    static boolean hasKeyPrefix(Entry e, String prefix) {
        return string(e.getKey()).startsWith(prefix);
    }

    static boolean hasKeyMatch(Entry e, String v) {
        return string(e.getKey()).equals(v);
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
