// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.support;

import java.math.BigDecimal;

public class Ulong implements Comparable<Ulong> {
    public static final Ulong ZERO = new Ulong(BigDecimal.ZERO, true);
    public static final Ulong ONE = new Ulong(BigDecimal.ONE, true);
    public static final Ulong MAX_VALUE = new Ulong("18446744073709551615", true);

    private BigDecimal value;
    private final boolean isConstant;

    private Ulong(String s, boolean isConstant) {
        value = new BigDecimal(s);
        this.isConstant = isConstant;
    }

    private Ulong(BigDecimal bd, boolean isConstant) {
        value = bd;
        this.isConstant = isConstant;
    }

    public Ulong(String s) {
        this.isConstant = false;
        if (!s.startsWith("-") && !s.contains(".")) {
            try {
                BigDecimal bd = new BigDecimal(s);
                if (MAX_VALUE.value.compareTo(bd) >= 0) {
                    value = bd;
                    return;
                }
            } catch (NumberFormatException nfe) { /* fall through */ }
        }
        throw invalid();
    }

    public Ulong(BigDecimal value) {
        this(value.toPlainString());
    }

    public Ulong(long value) {
        if (value < 0) {
            throw invalid();
        }
        this.value = BigDecimal.valueOf(value);
        this.isConstant = false;
    }

    private NumberFormatException invalid() {
        return new NumberFormatException("Ulong must be an integer between 0 and " + MAX_VALUE.toString() + " inclusive.");
    }

    @Override
    public String toString() {
        return value.toPlainString();
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof Ulong) && equalTo((Ulong) o);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public int compareTo(Ulong o) {
        return value.compareTo(o.value);
    }

    public BigDecimal value() {
        return value;
    }

    public boolean equalTo(Ulong u) {
        return value.compareTo(u.value) == 0;
    }

    public boolean lessThan(Ulong u) {
        return value.compareTo(u.value) < 0;
    }

    public boolean greaterThan(Ulong u) {
        return value.compareTo(u.value) > 0;
    }

    public void increment() {
        if (isConstant) {
            throw new IllegalStateException("Illegal to increment Ulong constant");
        }
        value = value.add(Ulong.ONE.value());
        if (value.compareTo(Ulong.MAX_VALUE.value) > 0) {
            value = Ulong.ZERO.value;
        }
    }

    public void decrement() {
        if (isConstant) {
            throw new IllegalStateException("Illegal to decrement Ulong constant");
        }
        value = value.subtract(Ulong.ONE.value());
        if (BigDecimal.ZERO.compareTo(value) > 0) {
            value = Ulong.MAX_VALUE.value;
        }
    }

    public Ulong add(Ulong augend) {
        BigDecimal bd = value.add(augend.value());
        if (bd.compareTo(Ulong.MAX_VALUE.value) > 0) {
            return new Ulong(bd.subtract(Ulong.MAX_VALUE.value).subtract(BigDecimal.ONE), false);
        }
        return new Ulong(bd, false);
    }

    public Ulong subtract(Ulong subtrahend) {
        if (subtrahend.greaterThan(this)) {
            return new Ulong(
                    Ulong.MAX_VALUE.value
                            .subtract(subtrahend.value.subtract(value))
                            .add(BigDecimal.ONE),
                    false);
        }
        return new Ulong(value.subtract(subtrahend.value), false);
    }
}
