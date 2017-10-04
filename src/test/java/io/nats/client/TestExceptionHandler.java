package io.nats.client;

/*
 * The MIT License
 *
 * Copyright 2017 Apcera, Inc..
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
/**
 *
 * @author Tim Boudreau
 */
public class TestExceptionHandler implements ExceptionHandler {

    Throwable thrown = null;

    synchronized void rethrow() throws Throwable {
        if (thrown != null) {
            throw thrown;
        }
    }

    volatile Thread testThread;

    void pickUpTestThread() {
        testThread = Thread.currentThread();
    }

    @Override
    public synchronized void onException(NATSException ex) {
        ex.printStackTrace(System.out);
        Throwable t = ex;
        while (t.getCause() != null) {
            t = t.getCause();
            if (t.getCause() == null) {
                break;
            }
        }
        if (thrown != null) {
            thrown.addSuppressed(t);
        } else {
            thrown = t;
        }
        if (testThread != null) {
            testThread.interrupt();
        }
    }
}
