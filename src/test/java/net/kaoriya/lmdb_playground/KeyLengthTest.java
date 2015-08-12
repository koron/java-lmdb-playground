package net.kaoriya.lmdb_playground;

import org.junit.Test;
import static org.junit.Assert.*;

import org.fusesource.lmdbjni.Env;

import static net.kaoriya.lmdb_playground.LMDBUtils.*;

public class KeyLengthTest {

  @Test
  public void testMaxKeySize() throws Exception {
    runNewEnv("tmp/test/KeyLength-testMaxKeySize", true, (env, db) -> {
        assertEquals(1023L, env.getMaxKeySize());
    });
  }

}
