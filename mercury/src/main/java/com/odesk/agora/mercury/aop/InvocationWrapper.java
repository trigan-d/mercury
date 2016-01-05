package com.odesk.agora.mercury.aop;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by Dmitry Solovyov on 01/05/2016.
 */
@FunctionalInterface
public interface InvocationWrapper<T, R> extends BiFunction<Function<T,R>, T, R> {
}
