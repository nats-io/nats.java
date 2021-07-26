package io.nats.client.support;

import java.math.BigDecimal;

public class Ulong implements Comparable<Ulong> {
    public static final Ulong ZERO = new Ulong("0", true);
    public static final Ulong ONE = new Ulong("1", true);
    public static final Ulong MAX_VALUE = new Ulong("18446744073709551615", true);

    private final BigDecimal value;

    private Ulong(String s, Object neededToMakeUniqueSignatureToInitializeStatics) {
        value = new BigDecimal(s);
    }

    public Ulong(String s) {
        if (s == null) {
            throw new NullPointerException();
        }
        if (!s.startsWith("-") && !s.contains(".")) {
            try {
                BigDecimal bd = new BigDecimal(s);
                if (MAX_VALUE.value.compareTo(bd) >= 0) {
                    value = bd;
                    return;
                }
            }
            catch (NumberFormatException nfe) { /* fall through */ }
        }
        throw invalid();
    }

    public Ulong(BigDecimal value) {
        this(value.toPlainString());
    }

    public Ulong(long value) {
        this(Long.toString(value));
    }

    private NumberFormatException invalid() {
        return new NumberFormatException("Ulong must be an integer between 0 and " + MAX_VALUE.toString() + " inclusive.");
    }

    public BigDecimal value() {
        return value;
    }

    public long longValue() {
        return value.longValue();
    }

    @Override
    public String toString() {
        return value.toPlainString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        return value.equals(((Ulong)o).value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public int compareTo(Ulong o) {
        return value.compareTo(o.value);
    }
}
