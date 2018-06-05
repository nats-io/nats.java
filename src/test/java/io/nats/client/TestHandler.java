// Copyright 2015-2018 The NATS Authors
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

public class TestHandler implements ErrorHandler, ConnectionHandler {
    private int count = 0;
    
    public void errorOccurred(Connection conn, Subscription sub, Errors type) {
        this.count = this.count + 1;
    }

    public void connectionEvent(Connection conn, Events type) {
        this.count = this.count + 1;
    }

    public int getCount() {
        return this.count;
    }
}