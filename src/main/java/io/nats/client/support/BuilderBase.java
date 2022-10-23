// Copyright 2022 The NATS Authors
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

import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.US_ASCII;

public abstract class BuilderBase {
    protected final Charset defaultCharset;
    protected int allocationSize;

    public static final int ALLOCATION_BOUNDARY = 32;
    public static final int DEFAULT_ASCII_ALLOCATION = 32;
    public static final int DEFAULT_OTHER_ALLOCATION = 64;
    public static final byte[] NULL = "null".getBytes(US_ASCII);

    protected BuilderBase(Charset defaultCharset) {
        this.defaultCharset = defaultCharset;
        this.allocationSize = _defaultCharsetAllocationSize();
    }

    protected BuilderBase(Charset defaultCharset, int allocationSize) {
        this.defaultCharset = defaultCharset;
        _setAllocationSize(allocationSize);
    }

    protected void _setAllocationSize(int allocationSizeSuggestion) {
        int dcas = _defaultCharsetAllocationSize();
        if (allocationSizeSuggestion <= dcas) {
            allocationSize = dcas;
        }
        else {
            allocationSize = bufferAllocSize(allocationSizeSuggestion, ALLOCATION_BOUNDARY);
        }
    }

    /**
     * Get the current allocation size
     * @return the allocation size
     */
    public int getAllocationSize() {
        return allocationSize;
    }

    private int _defaultCharsetAllocationSize() {
        return defaultCharset == US_ASCII ? DEFAULT_ASCII_ALLOCATION : DEFAULT_OTHER_ALLOCATION;
    }

    public static int bufferAllocSize(int atLeast, int blockSize) {
        return atLeast < blockSize
            ? blockSize
            : ((atLeast + blockSize) / blockSize) * blockSize;
    }
}
