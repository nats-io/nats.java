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

package io.nats.client.support;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Bridge to the NatsInetAddressProvider implementation
 */
public final class NatsInetAddress {
    private static NatsInetAddressProvider PROVIDER = new NatsInetAddressProvider() {};

    private NatsInetAddress() {} /* ensures cannot be constructed */

    /**
     * Set the provider. Null will reset to system default
     * @param provider the provider
     */
    public static void setProvider(final NatsInetAddressProvider provider) {
        PROVIDER = provider == null ? new NatsInetAddressProvider() {} : provider;
    }

    /**
     * Creates an InetAddress based on the provided host name and IP address.
     * @param host the specified host
     * @param addr the raw IP address in network byte order
     * @return  an InetAddress object created from the raw IP address.
     * @throws     UnknownHostException  if IP address is of illegal length
     */
    public static InetAddress getByAddress(String host, byte[] addr) throws UnknownHostException {
        return PROVIDER.getByAddress(host, addr);
    }

    /**
     * Determines the IP address of a host, given the host's name.
     * @param      host   the specified host, or {@code null}.
     * @return     an IP address for the given host name.
     * @throws     UnknownHostException  if no IP address for the
     *               {@code host} could be found, or if a scope_id was specified
     *               for a global IPv6 address.
     * @throws     SecurityException if a security manager exists
     *             and its checkConnect method doesn't allow the operation
     */
    public static InetAddress getByName(String host) throws UnknownHostException {
        return PROVIDER.getByName(host);
    }

    /**
     * Given the name of a host, returns an array of its IP addresses,
     * @param      host   the name of the host, or {@code null}.
     * @return     an array of all the IP addresses for a given host name.
     *
     * @throws     UnknownHostException  if no IP address for the
     *               {@code host} could be found, or if a scope_id was specified
     *               for a global IPv6 address.
     * @throws     SecurityException  if a security manager exists and its
     *               {@code checkConnect} method doesn't allow the operation.
     */
    public static InetAddress[] getAllByName(String host) throws UnknownHostException {
        return PROVIDER.getAllByName(host);
    }

    /**
     * Returns the loopback address.
     * @return  the InetAddress loopback instance.
     */
    public static InetAddress getLoopbackAddress() {
        return PROVIDER.getLoopbackAddress();
    }

    /**
     * Returns an {@code InetAddress} object given the raw IP address .
     * @param addr the raw IP address in network byte order
     * @return  an InetAddress object created from the raw IP address.
     * @throws     UnknownHostException  if IP address is of illegal length
     */
    public static InetAddress getByAddress(byte[] addr) throws UnknownHostException {
        return PROVIDER.getByAddress(addr);
    }

    /**
     * Returns the address of the local host.
     * @return     the address of the local host.
     * @throws     UnknownHostException  if the local host name could not
     *             be resolved into an address.
     */
    public static InetAddress getLocalHost() throws UnknownHostException {
        return PROVIDER.getLocalHost();
    }
}
