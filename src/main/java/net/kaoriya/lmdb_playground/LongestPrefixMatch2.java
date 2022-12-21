package net.kaoriya.lmdb_playground;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.GetOp;
import org.lmdbjava.Txn;

public class LongestPrefixMatch2 {

    private static ByteBuffer toByteBuffer(String s) {
        byte[] data = s.getBytes(UTF_8);
        ByteBuffer buf = ByteBuffer.allocateDirect(data.length);
        return buf.put(data).flip();
    }

    public static Map.Entry<ByteBuffer, ByteBuffer> match(Env<ByteBuffer> env, Dbi<ByteBuffer> db, String query) {
        return match(env, db, toByteBuffer(query));
    }

    public static Map.Entry<ByteBuffer, ByteBuffer> match(Env<ByteBuffer> env, Dbi<ByteBuffer> db, ByteBuffer query) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            return match(txn, db, query);
        }
    }

    public static Map.Entry<ByteBuffer, ByteBuffer> match(Txn<ByteBuffer> txn, Dbi<ByteBuffer> db, ByteBuffer query) {
        if (query == null) {
            return null;
        }
        query.rewind();
        Map.Entry<ByteBuffer, ByteBuffer> found = null;
        try (Cursor<ByteBuffer> c = db.openCursor(txn)) {
            for (int i = 1, lim = query.limit(); i <= lim; ++i) {
                ByteBuffer curr = query.slice().limit(i);
                if (!c.get(curr, GetOp.MDB_SET_RANGE)) {
                    break;
                }
                ByteBuffer key = c.key();
                int n = countPrefixMatch(curr, key);
                if (n < i) {
                    break;
                } else if (n >= i) {
                    i = n;
                    if (n == key.limit()) {
                        found = new AbstractMap.SimpleEntry<>(key.duplicate(), c.val().duplicate());
                    }
                }
            }
        }
        return found;
    }

    private static int countPrefixMatch(ByteBuffer s, ByteBuffer t) {
        int max = Math.min(s.limit(), t.limit());
        int i;
        for (i = 0; i < max; ++i) {
            if (s.get(i) != t.get(i)) {
                break;
            }
        }
        return i;
    }
}
