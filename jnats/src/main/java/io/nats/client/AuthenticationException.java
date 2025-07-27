// Copyright 2018 The NATS Authors
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

package io.nats.client;

import java.io.IOException;

/**
 * AuthenticationException is used when the connect process fails due to an authentication
 * problem.
 * 
 * The exception will not include the authentication tokens, but as a subclass
 * of IOException allows the client to distinguish an IO problem from an
 * authentication problem.
 */
public class AuthenticationException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new AuthenticationException.
     * 
     * @param errorMessage the error message, see Exception for details
     */
    public AuthenticationException(String errorMessage) {
        super(errorMessage);
    }
}