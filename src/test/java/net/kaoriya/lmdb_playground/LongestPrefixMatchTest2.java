package net.kaoriya.lmdb_playground;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;

import static net.kaoriya.lmdb_playground.LMDBUtils2.runNewEnv;
import static net.kaoriya.lmdb_playground.LongestPrefixMatch2.match;

public class LongestPrefixMatchTest2 {

    public static void assertMatch(
            Env<ByteBuffer> env,
            Dbi<ByteBuffer> db,
            String key,
            String expKey)
    {
        Map.Entry<ByteBuffer, ByteBuffer> entry = match(env, db, key);
        String actKey = entry == null ? null : LMDBUtils2.fromByteBuffer(entry.getKey());
        if (expKey == null) {
            if (actKey != null) {
                fail(String.format("%s expected no-entries, but found %s",
                            key, actKey));
            }
        } else {
            assertEquals(String.format("unmatch for key %s", key), expKey, actKey);
        }
    }

    private final static String[] TEST_SET01 = new String[]{
        "A", "C", "E",
        "AA", "AC", "AE", "CA", "CC", "CE", "EA", "EC", "EE",
        "AAA", "AAC", "AAE", "ACA", "ACC", "ACE", "AEA", "AEC", "AEE",
        "CAA", "CAC", "CAE", "CCA", "CCC", "CCE", "CEA", "CEC", "CEE",
        "EAA", "EAC", "EAE", "ECA", "ECC", "ECE", "EEA", "EEC", "EEE",
    };

    private static void putKeys(Dbi<ByteBuffer> db, String[] keys) {
        for (String key : keys) {
            LMDBUtils2.put(db, key, "auto_value:" + key);
        }
    }

    @Test
    public void basicMatch() {
        runNewEnv("tmp/test2/basicMatch", true, (env, db) -> {
            putKeys(db, TEST_SET01);
            assertMatch(env, db, "A", "A");
            assertMatch(env, db, "B", null);
            assertMatch(env, db, "C", "C");
            assertMatch(env, db, "D", null);
            assertMatch(env, db, "E", "E");
            assertMatch(env, db, "AA", "AA");
            assertMatch(env, db, "BB", null);
            assertMatch(env, db, "CC", "CC");
            assertMatch(env, db, "DD", null);
            assertMatch(env, db, "EE", "EE");
            assertMatch(env, db, "Z", null);
        });
    }

    @Test
    public void reverseMatch() {
        runNewEnv("tmp/test2/reverseMatch", true, (env, db) -> {
            putKeys(db, TEST_SET01);
            assertMatch(env, db, "AAB", "AA");
            assertMatch(env, db, "AAD", "AA");
            assertMatch(env, db, "ACB", "AC");
            assertMatch(env, db, "ACD", "AC");
        });
    }

    @Test
    public void adhocMatch01() {
        runNewEnv("tmp/test2/adhocMatch01", true, (env, db) -> {
            putKeys(db, new String[]{
                "foo",
                "foobar",
                "foobarbaz",
                "foofoo",
            });

            assertMatch(env, db, "foo", "foo");
            assertMatch(env, db, "foob", "foo");
            assertMatch(env, db, "fooba", "foo");
            assertMatch(env, db, "foobar", "foobar");
            assertMatch(env, db, "foobac", "foo");

            assertMatch(env, db, "foobarb", "foobar");
            assertMatch(env, db, "foobarc", "foobar");

            assertMatch(env, db, "fooc", "foo");
            assertMatch(env, db, "food", "foo");
            assertMatch(env, db, "foof", "foo");
            assertMatch(env, db, "foofo", "foo");
            assertMatch(env, db, "foofoo", "foofoo");

            assertMatch(env, db, "foofoofoo", "foofoo");
        });

    }

    @Test
    public void coverage01() {
        runNewEnv("tmp/test2/coverage01", true, (env, db) -> {
            putKeys(db, new String[]{
                "AA", "ABX", "ABY", "ZZZ",
            });
            assertMatch(env, db, "ABZ", null);
        });
    }

    @Test
    public void coverage02() {
        runNewEnv("tmp/test2/coverage02", true, (env, db) -> {
            putKeys(db, new String[]{
                "AA", "BBX", "BBY", "XXX", "YYY", "ZZZ",
            });
            assertMatch(env, db, "BBZ", null);
        });
    }

    @Test
    public void adhocMatch02() {
        runNewEnv("tmp/test2/adhocMatch02", true, (env, db) -> {
            putKeys(db, new String[]{
                "A", "AA", "ABX", "ABY", "ZZZ",
            });
            assertMatch(env, db, "ABZ", "A");
        });
    }
}
