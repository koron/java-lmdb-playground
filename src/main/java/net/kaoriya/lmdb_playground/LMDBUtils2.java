package net.kaoriya.lmdb_playground;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;

public class LMDBUtils2 {

    public static void runNewEnv(
            String path,
            boolean clean,
            BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> proc)
    {
        runNewEnv(new File(path), clean, 1024*1024, "default", proc);
    }

    public static void runNewEnv(
            File path,
            boolean clean,
            BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> proc)
    {
        runNewEnv(path, clean, 1024*1024, "default", proc);
    }

    public static void runNewEnv(
            File path,
            boolean clean,
            long mapSize,
            String name,
            BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> proc)
    {
        try (
            Env<ByteBuffer> env = newEnv(path, clean, mapSize);
        ) {
            Dbi<ByteBuffer> db = env.openDbi(name, DbiFlags.MDB_CREATE);
            proc.accept(env, db);
        }
    }

    public static Env<ByteBuffer> newEnv(File path, boolean clear, long mapSize) {
        if (clear) {
            deleteRecursively(path);
        }
        if (!path.exists()) {
            path.mkdirs();
        }
        return Env.create()
            .setMapSize(mapSize)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .open(path);
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

    public static ByteBuffer toByteBuffer(String s) {
        byte[] data = s.getBytes(UTF_8);
        ByteBuffer buf = ByteBuffer.allocateDirect(data.length);
        return buf.put(data).flip();
    }

    public static String fromByteBuffer(ByteBuffer buf) {
        if (buf == null) {
            return null;
        }
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return new String(bytes, UTF_8);
    }

    public static void put(Dbi<ByteBuffer> db, String key, String val) {

        db.put(toByteBuffer(key), toByteBuffer(val));
    }
}
