package net.kaoriya.lmdb_playground;

import org.junit.Test;
import static org.junit.Assert.*;

import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.Env;
import static org.fusesource.lmdbjni.Constants.string;

import static net.kaoriya.lmdb_playground.LongestPrefixMatch.match;
import static net.kaoriya.lmdb_playground.LMDBUtils.*;

public class LongestPrefixMatchTest {

    public static void assertMatch(Env env, Database db,
            String key, String expKey)
    {
        Entry entry = match(env, db, key);
        String actKey = entry != null ? string(entry.getKey()) : null;
        if (expKey == null) {
            if (actKey != null) {
                fail(String.format("%s expected no-entries, but found %s",
                            key, actKey));
            }
        } else {
            assertEquals(String.format("%s expected %s but found %s", key,
                        expKey, actKey), expKey, actKey);
        }
    }

    private final static String[] TEST_SET01 = new String[]{
        "A", "C", "E",
        "AA", "AC", "AE", "CA", "CC", "CE", "EA", "EC", "EE",
        "AAA", "AAC", "AAE", "ACA", "ACC", "ACE", "AEA", "AEC", "AEE",
        "CAA", "CAC", "CAE", "CCA", "CCC", "CCE", "CEA", "CEC", "CEE",
        "EAA", "EAC", "EAE", "ECA", "ECC", "ECE", "EEA", "EEC", "EEE",
    };

    @Test
    public void basicMatch() {
        runNewEnv("tmp/test/basicMatch", true, (env, db) -> {
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
        runNewEnv("tmp/test/reverseMatch", true, (env, db) -> {
            putKeys(db, TEST_SET01);
            assertMatch(env, db, "AAB", "AA");
            assertMatch(env, db, "AAD", "AA");
            assertMatch(env, db, "ACB", "AC");
            assertMatch(env, db, "ACD", "AC");
        });
    }

    @Test
    public void adhocMatch01() {
        runNewEnv("tmp/adhocMatch01", true, (env, db) -> {
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
        runNewEnv("tmp/test/coverage01", true, (env, db) -> {
            putKeys(db, new String[]{
                "AA", "ABX", "ABY", "ZZZ",
            });
            assertMatch(env, db, "ABZ", null);
        });
    }

    @Test
    public void coverage02() {
        runNewEnv("tmp/test/coverage02", true, (env, db) -> {
            putKeys(db, new String[]{
                "AA", "BBX", "BBY", "XXX", "YYY", "ZZZ",
            });
            assertMatch(env, db, "BBZ", null);
        });
    }

    @Test
    public void adhocMatch02() {
        runNewEnv("tmp/test/adhocMatch02", true, (env, db) -> {
            putKeys(db, new String[]{
                "A", "AA", "ABX", "ABY", "ZZZ",
            });
            assertMatch(env, db, "ABZ", "A");
        });
    }
}
