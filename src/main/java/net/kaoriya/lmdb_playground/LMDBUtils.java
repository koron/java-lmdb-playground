package net.kaoriya.lmdb_playground;

import java.io.File;
import java.io.IOException;
import java.util.function.BiConsumer;

import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;

import static org.fusesource.lmdbjni.Constants.bytes;

public class LMDBUtils {

    public static void runNewEnv (
            String dirPath,
            boolean clean,
            BiConsumer<Env, Database> proc)
    {
        runNewEnv(new File(dirPath), clean, proc);
    }

    public static void runNewEnv (
            File dir,
            boolean clean,
            BiConsumer<Env, Database> proc)
    {
        try (
            Env env = LMDBUtils.newEnv(dir, clean);
            Database db = env.openDatabase();
        ) {
            proc.accept(env, db);
        }
    }

    public static Env newEnv(String dirPath, boolean clear) {
        return newEnv(new File(dirPath), clear);
    }

    public static Env newEnv(File dir, boolean clear) {
        if (clear) {
            deleteRecursively(dir);
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

    private static void deleteRecursively(File file) {
        if (!file.exists()) {
            return;
        } else if (file.isFile()) {
            file.delete();
        } else if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                deleteRecursively(child);
            }
            file.delete();
        }
    }
}
