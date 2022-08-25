/*
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.

  Copyright (c) UXBOX Labs SL

  This file contains a UUIDv8 with conformance with
  https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format

  It has the following characteristics:
   - time ordered
   - 48bits timestamp (milliseconds precision, with custom epoch: 2022-01-01T00:00:00)
   - 14bits monotinical clockseq (allows 16384 ids/ms per host without blocking)
   - 60bits of randomnes generated statically on load
   - optional 12bits user defined host (that is part of the 60bits)

   This is results in a constantly increasing, sortable, very fast,
   and easy to visually read uuid implementation.
*/

package app.common;

import java.security.SecureRandom;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.time.Instant;

public class UUIDv8 {
  private static final long timeRef = 1640995200000L; // ms since 2022-01-01T00:00:00
  private static final Clock clock = Clock.systemUTC();
  private static final Lock lock = new ReentrantLock();
  private static final long baseMsb = 0x0000_0000_0000_8000L; // Version 8
  private static final long baseLsb = 0x8000_0000_0000_0000L; // Variant 2
  private static final long maxCs = 0x0000_0000_0000_3fff;

  private static final SecureRandom srandom = new java.security.SecureRandom();

  private static long lastCs = 0L;
  private static long lastTs = 0L;
  private static long lastRd = 0L;

  static {
    lastRd = srandom.nextLong();
  }

  public static UUID create(final long ts, final long lastRd, final long lastCs) {
    long msb = (baseMsb
                | (lastRd & 0xffff_ffff_ffff_0fffL));

    long lsb = (baseLsb
                | ((ts << 14) & 0x3fff_ffff_ffff_c000L)
                | (lastCs & 0x0000_0000_0000_3fffL));

    return new UUID(msb, lsb);
  }

  public static void setHost(final long host) {
    lock.lock();
    try {
      if (host > 0x0000_0000_0000_0fffL) {
        throw new IllegalArgumentException("host value should fit in 12bits");
      }

      lastRd = (lastRd
                & 0xffff_ffff_ffff_0000L
                | (host & 0x0000_0000_0000_0fffL));

    } finally {
      lock.unlock();
    }
  }

  public static Instant getTimestamp(final UUID uuid) {
    final long lsb = uuid.getLeastSignificantBits();
    return Instant.EPOCH.plusMillis(timeRef).plusMillis((lsb >>> 14) & 0x0000_ffff_ffff_ffffL);
  }

  public static UUID create() {
    lock.lock();
    try {
      while (true) {
        long ts = (clock.millis() - timeRef); // in seconds

        // If clock regression happens, regenerate lastRd
        if ((ts - lastTs) < 0) {
          // Clear and replace the first 48bits of randomness
          lastRd = (lastRd
                    & 0x0000_0000_0000_ffffL
                    | (srandom.nextLong() & 0xffff_ffff_ffff_0000L));

          lastCs = 0;
          continue;
        }

        // If last timestamp is the same as the current one we proceed
        // to increment the counters.
        if (lastTs == ts) {
          if (lastCs < maxCs) {
            lastCs++;
          } else {
            continue;
          }
        } else {
          lastTs = ts;
          lastCs = 0;
        }

        return create(ts, lastRd, lastCs);
      }
    } finally {
      lock.unlock();
    }
  }
}
