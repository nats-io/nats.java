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

package io.nats.client.channels;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.ListIterator;

import javax.net.ssl.SSLContext;
import java.security.GeneralSecurityException;

/**
 * Used to create instances of {@link NatsChannel}.
 */
@FunctionalInterface
public interface NatsChannelFactory {
    /**
     * A "sink" factory which has no need to delegate.
     */
    @FunctionalInterface
    interface Chain {
        NatsChannel connect(URI serverURI, Duration timeout) throws IOException, GeneralSecurityException;
        default public SSLContext createSSLContext(URI serverURI) throws GeneralSecurityException {
            return null;
        }
    }

    /**
     * Create a new NatsChannel for the given serverURI, options, and remaining timeout in nanoseconds.
     * 
     * @param serverURI is the URI of the server to connect to.
     * 
     * @param timeout is the max time that should elapse when attempting to connect.
     * 
     * @param next is the next factory in the chain of factories.
     * 
     * @return a new nats channel which is ready for reading and writing, otherwise an exception
     *     should be thrown to indicate why the connection could not be created.
     * 
     * @throws IOException if any IO error occurs.
     * @throws GeneralSecurityException if there is an issue creating the SSLContext.
     */
    public NatsChannel connect(URI serverURI, Duration timeout, Chain next) throws IOException, GeneralSecurityException;

    /**
     * Determine if this serverURI would require an SSLContext and if so, then build
     * an appropriate context.
     * 
     * @param serverURI is the URI to check if an SSLContext is required.
     * 
     * @param next is the next channel factory to use if delegating.
     * 
     * @throws GeneralSecurityException if a context can not be built.
     * 
     * @return a new SSLContext appropriate for this serverURI or null if
     *     it is not needed.
     */
    default public SSLContext createSSLContext(URI serverURI, Chain next) throws GeneralSecurityException {
        return next.createSSLContext(serverURI);
    }

    /**
     * Build out a chain from a list of factories with the specified final factory to
     * use.
     * 
     * @param factories is a list of factories that should be considered.
     * @param sink the final factory
     * @return the final built chain.
     */
    public static Chain buildChain(List<NatsChannelFactory> factories, Chain sink) {
        ListIterator<NatsChannelFactory> iter = factories.listIterator(factories.size());
        while (iter.hasPrevious()) {
            NatsChannelFactory factory = iter.previous();
            Chain[] next = new Chain[]{sink};
            sink = new Chain() {
                @Override
                public NatsChannel connect(URI serverURI, Duration timeout) throws IOException, GeneralSecurityException {
                    return factory.connect(serverURI, timeout, next[0]);
                }

                @Override
                public SSLContext createSSLContext(URI serverURI) throws GeneralSecurityException {
                    return factory.createSSLContext(serverURI, next[0]);
                }
            };
        }
        return sink;
    }
}
