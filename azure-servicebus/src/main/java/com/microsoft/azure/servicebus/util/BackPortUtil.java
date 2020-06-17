package com.microsoft.azure.servicebus.util;

import java8.util.function.IntUnaryOperator;
import java8.util.function.Predicate;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class BackPortUtil {
    public static int updateAndGet(AtomicInteger i, IntUnaryOperator operator) {
        int currentValue, afterApply;
        do {
            currentValue = i.get();
            afterApply = operator.applyAsInt(currentValue);
        } while (!i.compareAndSet(currentValue, afterApply));
        return afterApply;
    }

    public static <E> boolean removeIf(Collection<E> collection, Predicate<? super E> var1) {
        Objects.requireNonNull(var1);
        boolean ret = false;
        Iterator<E> iterator = collection.iterator();

        while(iterator.hasNext()) {
            if (var1.test(iterator.next())) {
                iterator.remove();
                ret = true;
            }
        }
        return ret;
    }
}
