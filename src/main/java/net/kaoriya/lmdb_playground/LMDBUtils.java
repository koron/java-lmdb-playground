package net.kaoriya.lmdb_playground;

import java.io.File;
import java.io.IOException;
import java.util.function.BiConsumer;

import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;

import static org.fusesource.lmdbjni.Constants.bytes;

/**
 * Utility functions for lmdbjni.
 */
public class LMDBUtils {

    /**
     * Start with new Env/Database.
     *
     * Make instances of Env and Database from dirPath, and call back proc with
     * those.
     *
     * If clean is true, all contents in dirPath will be deleted before making
     * instances.
     *
     * @param dirPath lmdb data directory.
     * @param clean if true,remove all contents of dirPath.
     * @param proc callback function to receive Env and Database.
     */
    public static void runNewEnv (
            String dirPath,
            boolean clean,
            BiConsumer<Env, Database> proc)
    {
        runNewEnv(new File(dirPath), clean, proc);
    }

    /**
     * Start with new Env/Database.
     *
     * Make instances of Env and Database from dir, and call back proc with
     * those.
     *
     * If clean is true, all contents in dir will be deleted before making
     * instances.
     *
     * @param dir lmdb data directory.
     * @param clean if true, remove all contents of dir.
     * @param proc callback function to receive Env and Database.
     */
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

    /**
     * Create a Env.
     *
     * @param dirPath lmdb data directory.
     * @param clear if true, remove all contents of dirPath.
     */
    public static Env newEnv(String dirPath, boolean clear) {
        return newEnv(new File(dirPath), clear);
    }

    /**
     * Create a Env.
     *
     * @param dir lmdb data directory.
     * @param clear if true, remove all contents of dir.
     */
    public static Env newEnv(File dir, boolean clear) {
        if (clear) {
            deleteRecursively(dir);
        }
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return new Env(dir.getPath());
    }

    /**
     * Put a pair of a string key and a string value to database.
     *
     * @param db instance of Database
     * @param key string key
     * @param val string value
     */
    public static void put(Database db, String key, String val) {
        db.put(bytes(key), bytes(val));
    }

    /**
     * Put pairs of a string key and a string value to database.
     *
     * @param db instance of Database
     * @param keyvals array of interleaved keys and values.
     */
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
