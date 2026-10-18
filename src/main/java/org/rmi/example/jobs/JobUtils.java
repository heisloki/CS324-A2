package org.rmi.example.jobs; // Same package as Job/JobResult

import java.util.List;


public final class JobUtils {

    private JobUtils() {} // static-only utility class, no instances

    // Accepts int[], Integer[], or List<? extends Number> and normalizes to int[].
    // Client GUIs may build jobs from parsed CSV data using any of these shapes.
    public static int[] toIntArray(Object data) {
        if (data instanceof int[]) {
            return (int[]) data;
        }
        if (data instanceof Integer[]) {
            Integer[] boxed = (Integer[]) data;
            int[] out = new int[boxed.length];
            for (int i = 0; i < boxed.length; i++) {
                out[i] = boxed[i];
            }
            return out;
        }
        if (data instanceof List<?>) {
            List<?> list = (List<?>) data;
            int[] out = new int[list.size()];
            for (int i = 0; i < list.size(); i++) {
                out[i] = ((Number) list.get(i)).intValue();
            }
            return out;
        }
        throw new IllegalArgumentException("Unsupported job data type: "
                + (data == null ? "null" : data.getClass().getName()));
    }

    //n < 2 is not prime by definition.
    public static boolean isPrime(long n) {
        if (n < 2) return false;
        if (n == 2) return true;
        if (n % 2 == 0) return false;
        for (long i = 3; i * i <= n; i += 2) {
            if (n % i == 0) return false;
        }
        return true;
    }
}
