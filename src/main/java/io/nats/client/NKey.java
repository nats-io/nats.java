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

import net.i2p.crypto.eddsa.EdDSAEngine;
import net.i2p.crypto.eddsa.EdDSAPrivateKey;
import net.i2p.crypto.eddsa.EdDSAPublicKey;
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveSpec;
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveTable;
import net.i2p.crypto.eddsa.spec.EdDSAPrivateKeySpec;
import net.i2p.crypto.eddsa.spec.EdDSAPublicKeySpec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.*;
import java.util.Arrays;

import static io.nats.client.support.RandomUtils.PRAND;
import static io.nats.client.support.RandomUtils.SRAND;

class DecodedSeed {
    int prefix;
    byte[] bytes;
}

/**
 * <p>
 * The NATS ecosystem will be moving to Ed25519 keys for identity,
 * authentication and authorization for entities such as Accounts, Users,
 * Servers and Clusters.
 * </p>
 * <p>
 * NKeys are based on the Ed25519 standard. This signing algorithm provides for
 * the use of public and private keys to sign and verify data. NKeys is designed
 * to formulate keys in a much friendlier fashion referencing work done in
 * cryptocurrencies, specifically Stellar. Bitcoin and others use a form of
 * Base58 (or Base58Check) to encode raw keys. Stellar utilizes a more
 * traditional Base32 with a CRC16 and a version or prefix byte. NKeys utilizes
 * a similar format with one or two prefix bytes. The base32 encoding of these
 * prefixes will yield friendly human readable prefixes, e.g. 'N' = server, 'C'
 * = cluster, 'O' = operator, 'A' = account, and 'U' = user to help developers
 * and administrators quickly identify key types.
 * </p>
 * <p>
 * Each NKey is generated from 32 bytes. These bytes are called the seed and are
 * encoded, in the NKey world, into a string starting with the letter 'S', with
 * a second character indicating the key’s type, e.g. "SU" is a seed for a u
 * er key pair, "SA" is a seed for an account key pair. The seed can be used t
 *  create the Ed25519 public/private key pair and should be protected as a p
 * ivate key. It is equivalent to the private key for a PGP key pair, or the m
 * ster password for your password vault.
 * </p>
 * <p>
 * Ed25519 uses the seed bytes to generate a key pair. The pair contains a
 * private key, which can be used to sign data, and a public key which can be
 * used to verify a signature. The public key can be distributed, and is not
 * considered secret.
 * </p>
 * <p>
 * The NKey libraries encode 32 byte public keys using Base32 and a CRC16
 * checksum plus a prefix based on the key type, e.g. U for a user key.
 * </p>
 * <p>
 * The NKey libraries have support for exporting a 64 byte private key. This
 * data is encoded into a string starting with the prefix ‘P’ for private. The
 * 64 bytes in a private key consists of the 32 bytes of the seed followed by 
 * he 32 bytes of the public key. Essentially, the private key is redundant sin
 * e you can get it back from the seed alone. The NATS team recommends sto
 * ing the 32 byte seed and letting the NKey library regenerate anything els
 *  it needs for signing.
 * </p>
 * <p>
 * The existence of both a seed and a private key can result in confusion. It is
 * reasonable to simply think of Ed25519 as having a public key and a private
 * seed, and ignore the longer private key concept. In fact, the NKey libraries
 * generally expect you to create an NKey from either a public key, to use for
 * verification, or a seed, to use for signing.
 * </p>
 * <p>
 * The NATS system will utilize public NKeys for identification, the NATS system
 * will never store or even have access to any private keys or seeds.
 * Authentication will utilize a challenge-response mechanism based on a
 * collection of random bytes called a nonce.
 * </p>
 * <p>
 * Version note - 2.2.0 provided string arguments for seeds, this is not as safe
 * as char arrays, so in 2.3.0 we have included a breaking change to char arrays.
 * While this is not the proper version choice, NKeys aren't widely used, if at all yet,
 * so we are making the change on a minor jump.
 * </p>
 */
public class NKey {

    /**
     * NKeys use a prefix byte to indicate their intended owner: 'N' = server, 'C' =
     * cluster, 'A' = account, and 'U' = user. 'P' is used for private keys. The
     * NKey class formalizes these into the enum NKey.Type.
     */
    public enum Type {
        /** A user NKey. */
        USER(PREFIX_BYTE_USER),
        /** An account NKey. */
        ACCOUNT(PREFIX_BYTE_ACCOUNT),
        /** A server NKey. */
        SERVER(PREFIX_BYTE_SERVER),
        /** An operator NKey. */
        OPERATOR(PREFIX_BYTE_OPERATOR),
        /** A cluster NKey. */
        CLUSTER(PREFIX_BYTE_CLUSTER),
        /** A private NKey. */
        PRIVATE(PREFIX_BYTE_PRIVATE);

        private final int prefix;

        Type(int prefix) {
            this.prefix = prefix;
        }

        public static Type fromPrefix(int prefix) {
            if (prefix == PREFIX_BYTE_ACCOUNT) {
                return ACCOUNT;
            } else if (prefix == PREFIX_BYTE_SERVER) {
                return SERVER;
            } else if (prefix == PREFIX_BYTE_USER) {
                return USER;
            } else if (prefix == PREFIX_BYTE_CLUSTER) {
                return CLUSTER;
            } else if (prefix == PREFIX_BYTE_PRIVATE) {
                return ACCOUNT;
            } else if (prefix == PREFIX_BYTE_OPERATOR) {
                return OPERATOR;
            }

            throw new IllegalArgumentException("Unknown prefix");
        }
    }

    // PrefixByteSeed is the prefix byte used for encoded NATS Seeds
    private static final int PREFIX_BYTE_SEED = 18 << 3; // Base32-encodes to 'S...'

    // PrefixBytePrivate is the prefix byte used for encoded NATS Private keys
    static final int PREFIX_BYTE_PRIVATE = 15 << 3; // Base32-encodes to 'P...'

    // PrefixByteServer is the prefix byte used for encoded NATS Servers
    static final int PREFIX_BYTE_SERVER = 13 << 3; // Base32-encodes to 'N...'

    // PrefixByteCluster is the prefix byte used for encoded NATS Clusters
    static final int PREFIX_BYTE_CLUSTER = 2 << 3; // Base32-encodes to 'C...'

    // PrefixByteAccount is the prefix byte used for encoded NATS Accounts
    static final int PREFIX_BYTE_ACCOUNT = 0; // Base32-encodes to 'A...'

    // PrefixByteUser is the prefix byte used for encoded NATS Users
    static final int PREFIX_BYTE_USER = 20 << 3; // Base32-encodes to 'U...'

    // PrefixByteOperator is the prefix byte used for encoded NATS Operators
    static final int PREFIX_BYTE_OPERATOR = 14 << 3; // Base32-encodes to 'O...'

    private static final int ED25519_PUBLIC_KEYSIZE = 32;
    private static final int ED25519_PRIVATE_KEYSIZE = 64;
    private static final int ED25519_SEED_SIZE = 32;
    private static final EdDSANamedCurveSpec ed25519 = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519);

    // XModem CRC based on the go version of NKeys
    private final static int[] crc16table = { 0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108,
            0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294,
            0x72f7, 0x62d6, 0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462, 0x3443, 0x0420,
            0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
            0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df,
            0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823, 0xc9cc, 0xd9ed,
            0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33,
            0x2a12, 0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a, 0x6ca6, 0x7c87, 0x4ce4, 0x5cc5,
            0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49, 0x7e97,
            0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70, 0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a,
            0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2,
            0x20e3, 0x5004, 0x4025, 0x7046, 0x6067, 0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
            0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e,
            0xe54f, 0xd52c, 0xc50d, 0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db, 0xb7fa,
            0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615,
            0x5634, 0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827,
            0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a, 0x4a75,
            0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b,
            0x9de8, 0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1, 0xef1f, 0xff3e, 0xcf5d,
            0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0 };

    static int crc16(byte[] bytes) {
        int crc = 0;

        for (byte b : bytes) {
            crc = ((crc << 8) & 0xffff) ^ crc16table[((crc >> 8) ^ (b & 0xFF)) & 0x00FF];
        }

        return crc;
    }

    // http://en.wikipedia.org/wiki/Base_32
    private static final String BASE32_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    private static final int[] BASE32_LOOKUP;
    private static final int MASK = 31;
    private static final int SHIFT = 5;

    static {
        BASE32_LOOKUP = new int[256];

        for (int i = 0; i < BASE32_LOOKUP.length; i++) {
            BASE32_LOOKUP[i] = 0xFF;
        }

        for (int i = 0; i < BASE32_CHARS.length(); i++) {
            int index = BASE32_CHARS.charAt(i) - '0';
            BASE32_LOOKUP[index] = i;
        }
    }

    static char[] base32Encode(final byte[] bytes) {
        int last = bytes.length;
        char[] charBuff = new char[(last + 7) * 8 / SHIFT];
        int offset = 0;
        int buffer = bytes[offset++];
        int bitsLeft = 8;
        int i = 0;

        while (bitsLeft > 0 || offset < last) {
            if (bitsLeft < SHIFT) {
                if (offset < last) {
                    buffer <<= 8;
                    buffer |= (bytes[offset++] & 0xff);
                    bitsLeft += 8;
                } else {
                    int pad = SHIFT - bitsLeft;
                    buffer <<= pad;
                    bitsLeft += pad;
                }
            }
            int index = MASK & (buffer >> (bitsLeft - SHIFT));
            bitsLeft -= SHIFT;
            charBuff[i] = BASE32_CHARS.charAt(index);
            i++;
        }

        int nonBlank;

        for (nonBlank=charBuff.length-1;nonBlank>=0;nonBlank--) {
            if (charBuff[nonBlank] != 0) {
                break;
            }
        }

        char[] retVal = new char[nonBlank+1];

        System.arraycopy(charBuff, 0, retVal, 0, retVal.length);

        for (int j=0;j<charBuff.length;j++) {
            charBuff[j] = '\0';
        }

        return retVal;
    }

    static byte[] base32Decode(final char[] input) {
        byte[] bytes = new byte[input.length * SHIFT / 8];
        int buffer = 0;
        int next = 0;
        int bitsLeft = 0;

        for (int i = 0; i < input.length; i++) {
            int lookup = input[i] - '0';

            if (lookup < 0 || lookup >= BASE32_LOOKUP.length) {
                continue;
            }

            int c = BASE32_LOOKUP[lookup];
            buffer <<= SHIFT;
            buffer |= c & MASK;
            bitsLeft += SHIFT;
            if (bitsLeft >= 8) {
                bytes[next++] = (byte) (buffer >> (bitsLeft - 8));
                bitsLeft -= 8;
            }
        }
        return bytes;
    }

    private static boolean checkValidPublicPrefixByte(int prefix) {
        switch (prefix) {
            case PREFIX_BYTE_SERVER:
            case PREFIX_BYTE_CLUSTER:
            case PREFIX_BYTE_OPERATOR:
            case PREFIX_BYTE_ACCOUNT:
            case PREFIX_BYTE_USER:
                return true;
        }
        return false;
    }

    static char[] removePaddingAndClear(char[] withPad) {
        int i;

        for (i=withPad.length-1;i>=0;i--) {
            if (withPad[i] != '=') {
                break;
            }
        }
        char[] withoutPad = new char[i+1];
        System.arraycopy(withPad, 0, withoutPad, 0, withoutPad.length);

        for (int j=0;j<withPad.length;j++) {
            withPad[j] = '\0';
        }

        return withoutPad;
    }

    static char[] encode(Type type, byte[] src) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        bytes.write(type.prefix);
        bytes.write(src);

        int crc = crc16(bytes.toByteArray());
        byte[] littleEndian = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) crc).array();

        bytes.write(littleEndian);

        char[] withPad = base32Encode(bytes.toByteArray());
        return removePaddingAndClear(withPad);
    }

    static char[] encodeSeed(Type type, byte[] src) throws IOException {
        if (src.length != ED25519_PRIVATE_KEYSIZE && src.length != ED25519_SEED_SIZE) {
            throw new IllegalArgumentException("Source is not the correct size for an ED25519 seed");
        }

        // In order to make this human printable for both bytes, we need to do a little
        // bit manipulation to setup for base32 encoding which takes 5 bits at a time.
        int b1 = PREFIX_BYTE_SEED | (type.prefix >> 5);
        int b2 = (type.prefix & 31) << 3; // 31 = 00011111

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        bytes.write(b1);
        bytes.write(b2);
        bytes.write(src);

        int crc = crc16(bytes.toByteArray());
        byte[] littleEndian = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) crc).array();

        bytes.write(littleEndian);

        char[] withPad = base32Encode(bytes.toByteArray());
        return removePaddingAndClear(withPad);
    }

    static byte[] decode(char[] src) {
        byte[] raw = base32Decode(src);

        if (raw == null || raw.length < 4) {
            throw new IllegalArgumentException("Invalid encoding for source string");
        }

        byte[] crcBytes = Arrays.copyOfRange(raw, raw.length - 2, raw.length);
        byte[] dataBytes = Arrays.copyOfRange(raw, 0, raw.length - 2);

        int crc = ByteBuffer.wrap(crcBytes).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;
        int actual = crc16(dataBytes);

        if (actual != crc) {
            throw new IllegalArgumentException("CRC is invalid");
        }

        return dataBytes;
    }

    static byte[] decode(Type expectedType, char[] src, boolean safe) {
        byte[] raw = decode(src);
        byte[] dataBytes = Arrays.copyOfRange(raw, 1, raw.length);
        Type type = NKey.Type.fromPrefix(raw[0] & 0xFF);

        if (type != expectedType) {
            if (safe) {
                return null;
            }
            throw new IllegalArgumentException("Unexpected type");
        }

        return dataBytes;
    }

    static DecodedSeed decodeSeed(char[] seed) {
        byte[] raw = decode(seed);

        // Need to do the reverse here to get back to internal representation.
        int b1 = raw[0] & 248; // 248 = 11111000
        int b2 = (raw[0] & 7) << 5 | ((raw[1] & 248) >> 3); // 7 = 00000111

        if (b1 != PREFIX_BYTE_SEED) {
            throw new IllegalArgumentException("Invalid encoding");
        }

        if (!checkValidPublicPrefixByte(b2)) {
            throw new IllegalArgumentException("Invalid encoded prefix byte");
        }

        byte[] dataBytes = Arrays.copyOfRange(raw, 2, raw.length);
        DecodedSeed retVal = new DecodedSeed();
        retVal.prefix = b2;
        retVal.bytes = dataBytes;
        return retVal;
    }

    private static NKey createPair(Type type, SecureRandom random)
            throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
        if (random == null) {
            random = SRAND;
        }

        byte[] seed = new byte[NKey.ed25519.getCurve().getField().getb() / 8];
        random.nextBytes(seed);

        return createPair(type, seed);
    }

    private static NKey createPair(Type type, byte[] seed)
            throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
        EdDSAPrivateKeySpec privKeySpec = new EdDSAPrivateKeySpec(seed, NKey.ed25519);
        EdDSAPrivateKey privKey = new EdDSAPrivateKey(privKeySpec);
        EdDSAPublicKeySpec pubKeySpec = new EdDSAPublicKeySpec(privKey.getA(), NKey.ed25519);
        EdDSAPublicKey pubKey = new EdDSAPublicKey(pubKeySpec);
        byte[] pubBytes = pubKey.getAbyte();

        byte[] bytes = new byte[pubBytes.length + seed.length];
        System.arraycopy(seed, 0, bytes, 0, seed.length);
        System.arraycopy(pubBytes, 0, bytes, seed.length, pubBytes.length);

        char[] encoded = encodeSeed(type, bytes);
        return new NKey(type, null, encoded);
    }

    /**
     * Create an Account NKey from the provided random number generator.
     * 
     * If no random is provided, SecureRandom() will be used to create one.
     * 
     * The new NKey contains the private seed, which should be saved in a secure location.
     * 
     * @param random A secure random provider
     * @return the new Nkey
     * @throws IOException if the seed cannot be encoded to a string
     * @throws NoSuchProviderException if the default secure random cannot be created
     * @throws NoSuchAlgorithmException if the default secure random cannot be created
     */
    public static NKey createAccount(SecureRandom random)
            throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
        return createPair(Type.ACCOUNT, random);
    }

    /**
     * Create an Cluster NKey from the provided random number generator.
     * 
     * If no random is provided, SecureRandom() will be used to create one.
     * 
     * The new NKey contains the private seed, which should be saved in a secure location.
     * 
     * @param random A secure random provider
     * @return the new Nkey
     * @throws IOException if the seed cannot be encoded to a string
     * @throws NoSuchProviderException if the default secure random cannot be created
     * @throws NoSuchAlgorithmException if the default secure random cannot be created
     */
    public static NKey createCluster(SecureRandom random)
            throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
        return createPair(Type.CLUSTER, random);
    }

    /**
     * Create an Operator NKey from the provided random number generator.
     * 
     * If no random is provided, SecureRandom() will be used to create one.
     * 
     * The new NKey contains the private seed, which should be saved in a secure location.
     * 
     * @param random A secure random provider
     * @return the new Nkey
     * @throws IOException if the seed cannot be encoded to a string
     * @throws NoSuchProviderException if the default secure random cannot be created
     * @throws NoSuchAlgorithmException if the default secure random cannot be created
     */
    public static NKey createOperator(SecureRandom random)
            throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
        return createPair(Type.OPERATOR, random);
    }

    /**
     * Create a Server NKey from the provided random number generator.
     * 
     * If no random is provided, SecureRandom() will be used to create one.
     * 
     * The new NKey contains the private seed, which should be saved in a secure location.
     * 
     * @param random A secure random provider
     * @return the new Nkey
     * @throws IOException if the seed cannot be encoded to a string
     * @throws NoSuchProviderException if the default secure random cannot be created
     * @throws NoSuchAlgorithmException if the default secure random cannot be created
     */
    public static NKey createServer(SecureRandom random)
            throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
        return createPair(Type.SERVER, random);
    }

    /**
     * Create a User NKey from the provided random number generator.
     * 
     * If no random is provided, SecureRandom() will be used to create one.
     * 
     * The new NKey contains the private seed, which should be saved in a secure location.
     * 
     * @param random A secure random provider
     * @return the new Nkey
     * @throws IOException if the seed cannot be encoded to a string
     * @throws NoSuchProviderException if the default secure random cannot be created
     * @throws NoSuchAlgorithmException if the default secure random cannot be created
     */
    public static NKey createUser(SecureRandom random)
            throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
        return createPair(Type.USER, random);
    }

    /**
     * Create an NKey object from the encoded public key. This NKey can be used for verification but not for signing.
     * 
     * @param publicKey the string encoded public key
     * @return the new Nkey
     */
    public static NKey fromPublicKey(char[] publicKey) {
        byte[] raw = decode(publicKey);
        int prefix = raw[0] & 0xFF;

        if (!checkValidPublicPrefixByte(prefix)) {
            throw new IllegalArgumentException("Not a valid public NKey");
        }

        Type type = NKey.Type.fromPrefix(prefix);
        return new NKey(type, publicKey, null);
    }

    /**
     * Creates an NKey object from a string encoded seed. This NKey can be used to sign or verify.
     * 
     * @param seed the string encoded seed, see {@link NKey#getSeed() getSeed()}
     * @return the Nkey
     */
    public static NKey fromSeed(char[] seed) {
        DecodedSeed decoded = decodeSeed(seed); // Should throw on bad seed

        if (decoded.bytes.length == ED25519_PRIVATE_KEYSIZE) {
            return new NKey(Type.fromPrefix(decoded.prefix), null, seed);
        } else {
            try {
                return createPair(Type.fromPrefix(decoded.prefix), decoded.bytes);
            } catch (Exception e) {
                throw new IllegalArgumentException("Bad seed value", e);
            }
        }
    }

    /**
     * @param src the encoded public key
     * @return true if the public key is an account public key
     */
    public static boolean isValidPublicAccountKey(char[] src) {
        return decode(Type.ACCOUNT, src, true) != null;
    }

    /**
     * @param src the encoded public key
     * @return true if the public key is a cluster public key
     */
    public static boolean isValidPublicClusterKey(char[] src) {
        return decode(Type.CLUSTER, src, true) != null;
    }

    /**
     * @param src the encoded public key
     * @return true if the public key is an operator public key
     */
    public static boolean isValidPublicOperatorKey(char[] src) {
        return decode(Type.OPERATOR, src, true) != null;
    }

    /**
     * @param src the encoded public key
     * @return true if the public key is a server public key
     */
    public static boolean isValidPublicServerKey(char[] src) {
        return decode(Type.SERVER, src, true) != null;
    }

    /**
     * @param src the encoded public key
     * @return true if the public key is a user public key
     */
    public static boolean isValidPublicUserKey(char[] src) {
        return decode(Type.USER, src, true) != null;
    }

    /**
     * The seed or private key per the Ed25519 spec, encoded with encodeSeed.
     */
    private char[] privateKeyAsSeed;

    /**
     * The public key, maybe null. Used for public only NKeys.
     */
    private char[] publicKey;

    private Type type;

    private NKey(Type t, char[] publicKey, char[] privateKey) {
        this.type = t;
        this.privateKeyAsSeed = privateKey;
        this.publicKey = publicKey;
    }

    /**
     * Clear the seed and public key char arrays by filling them
     * with random bytes then zero-ing them out.
     * 
     * The nkey is unusable after this operation.
     */
    public void clear() {
        if (privateKeyAsSeed != null) {
            for (int i=0; i< privateKeyAsSeed.length ; i++) {
                privateKeyAsSeed[i] = (char)(PRAND.nextInt(26) + 'a');
            }
            Arrays.fill(privateKeyAsSeed, '\0');
        }
        if (publicKey != null) {
            for (int i=0; i< publicKey.length ; i++) {
                publicKey[i] = (char)(PRAND.nextInt(26) + 'a');
            }
            Arrays.fill(publicKey, '\0');
        }
    }

    /**
     * @return the string encoded seed for this NKey
     */
    public char[] getSeed() {
        if (privateKeyAsSeed == null) {
            throw new IllegalStateException("Public-only NKey");
        }
        DecodedSeed decoded = decodeSeed(privateKeyAsSeed);
        byte[] seedBytes = new byte[ED25519_SEED_SIZE];
        System.arraycopy(decoded.bytes, 0, seedBytes, 0, seedBytes.length);
        try {
            return encodeSeed(Type.fromPrefix(decoded.prefix), seedBytes);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create seed.", e);
        }
    }

    /**
     * @return the encoded public key for this NKey
     * 
     * @throws GeneralSecurityException if there is an encryption problem
     * @throws IOException              if there is a problem encoding the public
     *                                  key
     */
    public char[] getPublicKey() throws GeneralSecurityException, IOException {
        if (publicKey != null) {
            return publicKey;
        }

        KeyPair keys = getKeyPair();
        EdDSAPublicKey pubKey = (EdDSAPublicKey) keys.getPublic();
        byte[] pubBytes = pubKey.getAbyte();

        return encode(this.type, pubBytes);
    }

    /**
     * @return the encoded private key for this NKey
     * 
     * @throws GeneralSecurityException if there is an encryption problem
     * @throws IOException              if there is a problem encoding the key
     */
    public char[] getPrivateKey() throws GeneralSecurityException, IOException {
        if (privateKeyAsSeed == null) {
            throw new IllegalStateException("Public-only NKey");
        }

        DecodedSeed decoded = decodeSeed(privateKeyAsSeed);
        return encode(Type.PRIVATE, decoded.bytes);
    }

    /**
     * @return A Java security keypair that represents this NKey in Java security
     *         form.
     * 
     * @throws GeneralSecurityException if there is an encryption problem
     * @throws IOException              if there is a problem encoding or decoding
     */
    public KeyPair getKeyPair() throws GeneralSecurityException, IOException {
        if (privateKeyAsSeed == null) {
            throw new IllegalStateException("Public-only NKey");
        }

        DecodedSeed decoded = decodeSeed(privateKeyAsSeed);
        byte[] seedBytes = new byte[ED25519_SEED_SIZE];
        byte[] pubBytes = new byte[ED25519_PUBLIC_KEYSIZE];

        System.arraycopy(decoded.bytes, 0, seedBytes, 0, seedBytes.length);
        System.arraycopy(decoded.bytes, seedBytes.length, pubBytes, 0, pubBytes.length);

        EdDSAPrivateKeySpec privKeySpec = new EdDSAPrivateKeySpec(seedBytes, NKey.ed25519);
        EdDSAPrivateKey privKey = new EdDSAPrivateKey(privKeySpec);
        EdDSAPublicKeySpec pubKeySpec = new EdDSAPublicKeySpec(pubBytes, NKey.ed25519);
        EdDSAPublicKey pubKey = new EdDSAPublicKey(pubKeySpec);

        return new KeyPair(pubKey, privKey);
    }

    /**
     * @return the Type of this NKey
     */
    public Type getType() {
        return type;
    }

    /**
     * Sign aribitrary binary input.
     * 
     * @param input the bytes to sign
     * @return the signature for the input from the NKey
     * 
     * @throws GeneralSecurityException if there is an encryption problem
     * @throws IOException              if there is a problem reading the data
     */
    public byte[] sign(byte[] input) throws GeneralSecurityException, IOException {
        Signature sgr = new EdDSAEngine(MessageDigest.getInstance(NKey.ed25519.getHashAlgorithm()));
        PrivateKey sKey = getKeyPair().getPrivate();

        sgr.initSign(sKey);
        sgr.update(input);

        return sgr.sign();
    }

    /**
     * Verify a signature.
     * 
     * @param input     the bytes that were signed
     * @param signature the bytes for the signature
     * @return true if the signature matches this keys signature for the input.
     * 
     * @throws GeneralSecurityException if there is an encryption problem
     * @throws IOException              if there is a problem reading the data
     */
    public boolean verify(byte[] input, byte[] signature) throws GeneralSecurityException, IOException {
        Signature sgr = new EdDSAEngine(MessageDigest.getInstance(NKey.ed25519.getHashAlgorithm()));
        PublicKey sKey = null;

        if (privateKeyAsSeed != null) {
            sKey = getKeyPair().getPublic();
        } else {
            char[] encodedPublicKey = getPublicKey();
            byte[] decodedPublicKey = decode(this.type, encodedPublicKey, false);
            EdDSAPublicKeySpec pubKeySpec = new EdDSAPublicKeySpec(decodedPublicKey, NKey.ed25519);
            sKey = new EdDSAPublicKey(pubKeySpec);
        }

        sgr.initVerify(sKey);
        sgr.update(input);

        return sgr.verify(signature);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof NKey)) {
            return false;
        }

        NKey otherNKey = (NKey) o;

        if (this.type != otherNKey.type) {
            return false;
        }

        if (this.privateKeyAsSeed == null) {
            return Arrays.equals(this.publicKey, otherNKey.publicKey);
        }

        return Arrays.equals(this.privateKeyAsSeed, otherNKey.privateKeyAsSeed);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.type.prefix;

        if (this.privateKeyAsSeed == null) {
            result = 31 * result + Arrays.hashCode(this.publicKey);
        } else {
            result = 31 * result + Arrays.hashCode(this.privateKeyAsSeed);
        }
        return result;
    }

}