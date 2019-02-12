package io.nats.client;

import java.util.Objects;

@FunctionalInterface
public interface Predicate<T> {
    boolean test(T var1);

    default java.util.function.Predicate<T> and(java.util.function.Predicate<? super T> var1) {
        Objects.requireNonNull(var1);
        return (var2) -> {
            return this.test(var2) && var1.test(var2);
        };
    }

    default java.util.function.Predicate<T> negate() {
        return (var1) -> {
            return !this.test(var1);
        };
    }

    default java.util.function.Predicate<T> or(java.util.function.Predicate<? super T> var1) {
        Objects.requireNonNull(var1);
        return (var2) -> {
            return this.test(var2) || var1.test(var2);
        };
    }

    static <T> java.util.function.Predicate<T> isEqual(Object var0) {
        return null == var0 ? Objects::isNull : (var1) -> {
            return var0.equals(var1);
        };
    }
}