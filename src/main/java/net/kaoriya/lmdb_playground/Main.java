package net.kaoriya.lmdb_playground;

import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.EntryIterator;
import org.fusesource.lmdbjni.Env;

import static org.fusesource.lmdbjni.Constants.bytes;
import static org.fusesource.lmdbjni.Constants.string;

public class Main {
    public static void main(String[] args) {
        try (Env env = new Env("./tmp/mydb")) {
            try (Database db = env.openDatabase()) {
                playSeek(db, "bbb");
            }
        }
    }

    static void playSeek(Database db, String prefix) {
        System.out.println("prefix=" + prefix);
        // seek returns valid iter when key matches exactly.
        try (EntryIterator it = db.seek(bytes(prefix))) {
            for (Entry e : it.iterable()) {
                printEntry(e, "  ");
            }
        }
    }

    static void playIter(Database db) {
        try (EntryIterator it = db.iterate()) {
            for (Entry e : it.iterable()) {
                printEntry(e);
            }
        }
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
