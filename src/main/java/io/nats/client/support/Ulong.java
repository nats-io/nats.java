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
    public static final Ulong ZERO = new Ulong("0", true);
    public static final Ulong ONE = new Ulong("1", true);
    public static final Ulong MAX_VALUE = new Ulong("18446744073709551615", true);

    private final BigDecimal value;

    private Ulong(String s, Object neededToMakeUniqueSignatureToInitializeStatics) {
        value = new BigDecimal(s);
    }

    public Ulong(String s) {
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
        if (value < 0) {
            throw invalid();
        }
        this.value = BigDecimal.valueOf(value);
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
        return (o instanceof Ulong) && equalTo((Ulong)o);
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
}
