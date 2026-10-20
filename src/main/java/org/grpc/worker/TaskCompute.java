package org.grpc.worker;

import ds.grpc.JobType;

import java.util.function.BooleanSupplier;

/**
 * The actual computation of ONE task, with checkpoints. No gRPC in here, so it can be tested alone.
 *
 * A task covers either a slice of a number list (MAX, PRIMECOUNT) or a sub-range (PRIMESUM).
 * While it runs it reports (items processed, running result). That pair is a CHECKPOINT: if this
 * worker dies, the coordinator hands the task to another worker which resumes from the checkpoint
 * instead of starting again, so no finished work is repeated.
 */
public final class TaskCompute {

    /** Receives progress. {@code finished} is true exactly once, at the end, and partialValue is then the final result. */
    public interface Sink {
        void update(long processed, long partialValue, boolean finished);
    }

    private static final long EMIT_EVERY_NANOS = 100_000_000L; // at most ~10 updates per second
    private static final int CHECK_MASK = 1023;                // look at the clock every 1024 items

    private TaskCompute() { }

    /** Number of items this task covers. */
    public static long total(JobType type, int[] numbers, int rangeStart, int rangeEnd) {
        return type == JobType.JOB_TYPE_PRIMESUM ? (long) rangeEnd - rangeStart + 1 : numbers.length;
    }

    /**
     * Runs the task, starting at {@code resumeOffset} (0 = from scratch) with the running result
     * {@code resumeValue} that the previous worker had reached there.
     *
     * @return true if it ran to the end, false if {@code cancelled} turned true first
     */
    public static boolean run(JobType type, int[] numbers, int rangeStart, int rangeEnd,
                              long resumeOffset, long resumeValue,
                              BooleanSupplier cancelled, Sink sink) {
        final long total = total(type, numbers, rangeStart, rangeEnd);
        final long startAt = Math.max(0, Math.min(resumeOffset, total));

        long value;
        if (startAt > 0) {
            value = resumeValue;                                   // continue from the checkpoint
        } else {
            value = type == JobType.JOB_TYPE_MAX ? Long.MIN_VALUE : 0;
        }

        sink.update(startAt, value, false); // "I have started" (also tells the coordinator which worker runs it)
        long lastEmit = System.nanoTime();

        for (long i = startAt; i < total; i++) {
            long n = type == JobType.JOB_TYPE_PRIMESUM ? rangeStart + i : numbers[(int) i];
            switch (type) {
                case JOB_TYPE_MAX:
                    if (n > value) {
                        value = n;
                    }
                    break;
                case JOB_TYPE_PRIMESUM:
                    if (isPrime(n)) {
                        value += n;
                    }
                    break;
                case JOB_TYPE_PRIMECOUNT:
                    if (isPrime(n)) {
                        value++;
                    }
                    break;
                default:
                    throw new IllegalArgumentException("unsupported job type " + type);
            }
            if ((i & CHECK_MASK) == CHECK_MASK) {
                if (cancelled.getAsBoolean()) {
                    return false;
                }
                long now = System.nanoTime();
                if (now - lastEmit >= EMIT_EVERY_NANOS) {
                    sink.update(i + 1, value, false);
                    lastEmit = now;
                }
            }
        }
        sink.update(total, value, true);
        return true;
    }

    /** n < 2 is not prime by definition. */
    public static boolean isPrime(long n) {
        if (n < 2) {
            return false;
        }
        if (n == 2) {
            return true;
        }
        if (n % 2 == 0) {
            return false;
        }
        for (long i = 3; i * i <= n; i += 2) {
            if (n % i == 0) {
                return false;
            }
        }
        return true;
    }
}
