package net.kaoriya.lmdb_playground;

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

public class Simple {
    public static void main(String[] args) {
        try (
            Env env = new Env("./tmp/mydb");
            Database db = env.openDatabase()
        ) {
            playSeek2(env, db, "bb");
        }
    }

    static void playSeek2(Env env, Database db, String prefix) {
        System.out.println("seek2: prefix=" + prefix);
        try (
            Transaction tx = env.createTransaction(true);
            Cursor c = db.openCursor(tx);
        ) {
            Entry e = c.seek(SeekOp.RANGE, bytes(prefix));
            while (e != null && hasPrefix(e, prefix)) {
                printEntry(e, "  ");
                e = c.get(GetOp.NEXT);
            }
        }
    }

    static boolean hasPrefix(Entry e, String prefix) {
        return string(e.getKey()).startsWith(prefix);
    }

    static void printEntry(Entry entry) {
        printEntry(entry, "");
    }

    static void printEntry(Entry entry, String header) {
        String k = string(entry.getKey());
        String v = string(entry.getValue());
        System.out.println(header + "key=" + k + " value=" + v);
    }

    static void playPut(Database db) {
        put(db, "aaa", "v:aaa");
        put(db, "aab", "v:aab");
        put(db, "aac", "v:aac");
        put(db, "aba", "v:aba");
        put(db, "abb", "v:abb");
        put(db, "abc", "v:abc");
        put(db, "aca", "v:aca");
        put(db, "acb", "v:acb");
        put(db, "acc", "v:acc");
        put(db, "baa", "v:baa");
        put(db, "bab", "v:bab");
        put(db, "bac", "v:bac");
        put(db, "bba", "v:bba");
        put(db, "bbb", "v:bbb");
        put(db, "bbc", "v:bbc");
        put(db, "bca", "v:bca");
        put(db, "bcb", "v:bcb");
        put(db, "bcc", "v:bcc");
        put(db, "caa", "v:caa");
        put(db, "cab", "v:cab");
        put(db, "cac", "v:cac");
        put(db, "cba", "v:cba");
        put(db, "cbb", "v:cbb");
        put(db, "cbc", "v:cbc");
        put(db, "cca", "v:cca");
        put(db, "ccb", "v:ccb");
        put(db, "ccc", "v:ccc");
    }

    static void put(Database db, String key, String value) {
        db.put(bytes(key), bytes(value));
    }

}
