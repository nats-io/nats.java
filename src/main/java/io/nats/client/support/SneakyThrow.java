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

/**
 * Sometimes you want to throw a checked exception, but you can't since you
 * are in the confines of an interface which does NOT throw this checked
 * exception.
 * 
 * You could chain the exception and have ugly "caused by" stack traces and
 * more boiler-plate code.
 * 
 * Or you can use sneaky throws to defeat the compiler.
 */
public interface SneakyThrow {
    static RuntimeException USELESS_EXCEPTION = new RuntimeException();

    /**
     * Usage:
     * 
     * <pre>
     * throw sneakyThrow(new Exception())
     * </pre>
     * 
     * This function returns a runtime exception so you can trick the compiler
     * into thinking that your code will throw a runtime exception rather than
     * a potentially checked exception.
     * 
     * @param <E> temp type used for casting the exception.
     * @param ex is an arbitrary exception that you want to throw
     * @return a fake RuntimeException so you can teach the compiler that no
     *     subsequent code is reachable by `throw`ing this fake exception.
     * @throws E is the type used for casting the real exception
     */
    @SuppressWarnings("unchecked")
    static <E extends Throwable> RuntimeException sneakyThrow(Throwable ex) throws E {
        if (true) {
            throw (E) ex;
        }
        return USELESS_EXCEPTION;
    }
}
