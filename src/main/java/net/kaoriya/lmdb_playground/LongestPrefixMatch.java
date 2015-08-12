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

    public static Entry match(Env env, Database db, String s) {
        Transaction tx = env.createTransaction(true);
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
