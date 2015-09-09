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
