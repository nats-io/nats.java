// Copyright 2025 The NATS Authors
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

package io.nats.client.utils;

import io.nats.client.Connection;
import io.nats.client.api.ServerInfo;

public abstract class VersionUtils {
    public static ServerInfo VERSION_SERVER_INFO;

    public interface VersionCheck {
        boolean runTest(ServerInfo si);
    }

    public static void initVersionServerInfo(Connection nc) {
        if (VERSION_SERVER_INFO == null) {
            VERSION_SERVER_INFO = nc.getServerInfo();
        }
    }

    public static boolean atLeast2_10() {
        return atLeast2_10(VERSION_SERVER_INFO);
    }

    public static boolean atLeast2_10(ServerInfo si) {
        return si.isNewerVersionThan("2.9.99");
    }

    public static boolean atLeast2_10_3(ServerInfo si) {
        return si.isSameOrNewerThanVersion("2.10.3");
    }

    public static boolean atLeast2_10_26(ServerInfo si) {
        return si.isSameOrNewerThanVersion("2.10.26");
    }

    public static boolean atLeast2_11(ServerInfo si) {
        return si.isNewerVersionThan("2.10.99");
    }

    public static boolean before2_11() {
        return before2_11(VERSION_SERVER_INFO);
    }

    public static boolean before2_11(ServerInfo si) {
        return si.isOlderThanVersion("2.11");
    }

    public static boolean atLeast2_12() {
        return atLeast2_12(VERSION_SERVER_INFO);
    }

    public static boolean atLeast2_12(ServerInfo si) {
        return si.isSameOrNewerThanVersion("2.11.99");
    }
}
