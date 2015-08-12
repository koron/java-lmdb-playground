package net.kaoriya.lmdb_playground;

import java.io.File;
import java.io.IOException;
import java.util.function.BiConsumer;

import org.apache.commons.io.FileUtils;

import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;

import static org.fusesource.lmdbjni.Constants.bytes;
import static org.fusesource.lmdbjni.Constants.string;

public class LMDBUtils {

    public static void runNewEnv (
            String path,
            boolean clean,
            BiConsumer<Env, Database> proc)
    {
        try (
            Env env = LMDBUtils.newEnv(path, clean);
            Database db = env.openDatabase();
        ) {
            proc.accept(env, db);
        }
    }

    public static Env newEnv(String path, boolean clear) {
        File dir = new File(path);
        if (clear) {
            try {
                FileUtils.deleteDirectory(dir);
            } catch (IOException e) {
            }
        }
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return new Env(dir.getPath());
    }

    public static void put(Database db, String key, String val) {
        db.put(bytes(key), bytes(val));
    }

    public static void put(Database db, String[] keyvals) {
        for (int i = 0; i + 1 < keyvals.length; i += 2) {
            put(db, keyvals[i], keyvals[i + 1]);
        }
    }

    public static void putKeys(Database db, String[] keys) {
        for (String key : keys) {
            put(db, key, "auto_value:" + key);
        }
    }
}
