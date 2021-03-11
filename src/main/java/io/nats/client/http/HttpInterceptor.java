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

package io.nats.client.http;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.ListIterator;

/**
 * Implement this interface if you want to augment anything about the
 * http request/response flow.
 */
public interface HttpInterceptor {
    interface Chain {
        /**
         * Access the underlying writer channel, useful when implementing proxy
         * support or oauth support (for example).
         * 
         * @return the underlying byte channel used for writes.
         */
        GatheringByteChannel writerChannel();

        /**
         * Access the underlying reader channel, useful when implementing proxy
         * support or oauth support (for example).
         * 
         * @return the underlying byte channel used for reads.
         */
        ReadableByteChannel readerChannel();

        /**
         * The buffer which contains any residual results from
         * the last read.
         * 
         * @return the reader buffer, always in "get" mode
         */
        ByteBuffer readerBuffer();

        /**
         * Proceed to the next interceptor in the chain.
         * 
         * @param request is the request to use in the next stage.
         * @return the response from the next stage.
         * @throws IOException since the final link in the chain will
         *   need to perform IO operations which may throw this exception.
         */
        HttpResponse proceed(HttpRequest request) throws IOException;
    }

    /**
     * Implement this interface in order to intercept requests.
     * 
     * @param request is the current request being used
     * @param next is the next interceptor or the final element in the chain.
     *    writing of the request to the underlying channel.
     * @return the desired response
     * @throws IOException since {@link Chain#proceed(HttpRequest)} may
     *    throw this exception.
     */
    HttpResponse intercept(HttpRequest request, Chain next) throws IOException;

    /**
     * Build out a chain from a list of interceptors that are backed by a specified
     * channel. The final implementation in the chain writes the request to the channel
     * and reads the response from that same channel.
     * 
     * @param httpInterceptors is a list of interceptors
     * @param writer is the channel used for writes
     * @param reader is the channel used for reads
     * @param readerBuffer is in "get" mode and contains any residual network bytes from the last read
     * @return the final built chain.
     */
    public static Chain buildChain(List<HttpInterceptor> httpInterceptors, GatheringByteChannel writer, ReadableByteChannel reader, ByteBuffer readerBuffer) {
        Chain chain = new HttpInterceptor.Chain() {
            @Override
            public GatheringByteChannel writerChannel() {
                return writer;
            }

            @Override
            public ReadableByteChannel readerChannel() {
                return reader;
            }

            @Override
            public ByteBuffer readerBuffer() {
                return readerBuffer;
            }

            @Override
            public HttpResponse proceed(HttpRequest request) throws IOException {
                request.write(writer);
                return HttpResponse.read(reader, readerBuffer);
            }
        };
        ListIterator<HttpInterceptor> iter = httpInterceptors.listIterator(httpInterceptors.size());
        while (iter.hasPrevious()) {
            HttpInterceptor interceptor = iter.previous();
            HttpInterceptor.Chain[] next = new HttpInterceptor.Chain[]{chain};
            chain = new HttpInterceptor.Chain() {
                @Override
                public GatheringByteChannel writerChannel() {
                    return writer;
                }
    
                @Override
                public ReadableByteChannel readerChannel() {
                    return reader;
                }
    
                @Override
                public ByteBuffer readerBuffer() {
                    return readerBuffer;
                }
    
                    @Override
                public HttpResponse proceed(HttpRequest request) throws IOException {
                    return interceptor.intercept(request, next[0]);
                }
            };
        }
        return chain;
    }
}
