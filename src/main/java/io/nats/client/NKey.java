package io.nats.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.util.Arrays;

import org.apache.commons.codec.binary.Base32;

import net.i2p.crypto.eddsa.EdDSAEngine;
import net.i2p.crypto.eddsa.EdDSAPrivateKey;
import net.i2p.crypto.eddsa.EdDSAPublicKey;
import net.i2p.crypto.eddsa.math.GroupElement;
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveSpec;
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveTable;
import net.i2p.crypto.eddsa.spec.EdDSAPrivateKeySpec;
import net.i2p.crypto.eddsa.spec.EdDSAPublicKeySpec;

class DecodedSeed {
    int prefix;
    byte[] bytes;
}

public class NKey {

    /**
     * NKeys use a prefix byte to indicate their intended owner: 
     * 'N' = server, 'C' = cluster, 'A' = account, and 'U' = user. 
     * 'P' is used for private keys. The NKey class formalizes these
     * into the enum NKey.Type.
     */
    public enum Type {
        /** A user NKey. */
        USER (PREFIX_BYTE_USER),
        /** An account NKey. */
        ACCOUNT (PREFIX_BYTE_ACCOUNT),
        /** A server NKey. */
        SERVER (PREFIX_BYTE_SERVER),
        /** A cluster NKey. */
        CLUSTER (PREFIX_BYTE_CLUSTER),
        /** A private NKey. */
        PRIVATE (PREFIX_BYTE_PRIVATE);

        private final int prefix;
        Type(int prefix) {
            this.prefix = prefix;
        }

        public static Type fromPrefix(int prefix) {
            if (prefix == PREFIX_BYTE_ACCOUNT) {
                return ACCOUNT;
            } else if (prefix == PREFIX_BYTE_SERVER) {
                return SERVER;
            }  else if (prefix == PREFIX_BYTE_USER) {
                return USER;
            }  else if (prefix == PREFIX_BYTE_CLUSTER) {
                return CLUSTER;
            }  else if (prefix == PREFIX_BYTE_PRIVATE) {
                return ACCOUNT;
            }
            
            throw new IllegalArgumentException("Unknown prefix");
        }
    }
    
    //PrefixByteSeed is the version byte used for encoded NATS Seeds
	private static int PREFIX_BYTE_SEED = 18 << 3; // Base32-encodes to 'S...'

	//PrefixBytePrivate is the version byte used for encoded NATS Private keys
	private static int PREFIX_BYTE_PRIVATE = 15 << 3; // Base32-encodes to 'P...'

	//PrefixByteServer is the version byte used for encoded NATS Servers
	private static int PREFIX_BYTE_SERVER = 13 << 3; // Base32-encodes to 'N...'

	//PrefixByteCluster is the version byte used for encoded NATS Clusters
	private static int PREFIX_BYTE_CLUSTER = 2 << 3; // Base32-encodes to 'C...'

	//PrefixByteAccount is the version byte used for encoded NATS Accounts
	private static int PREFIX_BYTE_ACCOUNT = 0; // Base32-encodes to 'A...'

	//PrefixByteUser is the version byte used for encoded NATS Users
    private static int PREFIX_BYTE_USER = 20 << 3; // Base32-encodes to 'U...'

    private static final int ED25519_PUBLIC_KEYSIZE = 32;
    private static final int ED25519_PRIVATE_KEYSIZE = 64;
    private static final int ED25519_SEED_SIZE = 32;
    private static final EdDSANamedCurveSpec ed25519 = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519);
    
    // XModem CRC based on the go version of NKeys
    private final static int[] crc16table = {
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
        0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
        0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
        0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
        0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
        0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
        0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
        0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
        0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
        0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
        0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
        0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
        0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
        0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
        0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
        0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
        0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
        0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
        0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
        0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
        0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
        0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
        0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
        0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
        0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
        0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
        0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
        0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
        0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
        0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
        0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
    };
    
    public static int crc16(byte[] bytes) {
        int crc = 0;
    
        for (byte b : bytes) {
		    crc = ((crc << 8) & 0xffff) ^ crc16table[((crc>>8) ^ (b & 0xFF)) & 0x00FF];
        }

        return crc;
    }
    
    private static boolean checkValidPublicPrefixByte(int prefix) {
        if (prefix == PREFIX_BYTE_SERVER) {
            return true;
        }
        if (prefix == PREFIX_BYTE_CLUSTER) {
            return true;
        }
        if (prefix == PREFIX_BYTE_ACCOUNT) {
            return true;
        }
        if (prefix == PREFIX_BYTE_USER) {
            return true;
        }
        return false;
    }

    public static String encode(Type type, byte[] src) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        bytes.write(type.prefix);
        bytes.write(src);
    
        int crc = crc16(bytes.toByteArray());
        byte[] littleEndian = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short)crc).array();

        bytes.write(littleEndian);
        
        Base32 encoder = new Base32();
        String withPad = encoder.encodeToString(bytes.toByteArray());
        String withoutPad = withPad.replace("=", "");
        return withoutPad;
    }

    public static String encodeSeed(Type type, byte[] src) throws IOException {
        if (src.length != ED25519_PRIVATE_KEYSIZE) {
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
        byte[] littleEndian = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short)crc).array();

        bytes.write(littleEndian);
        
        Base32 encoder = new Base32();
        String withPad = encoder.encodeToString(bytes.toByteArray());
        String withoutPad = withPad.replace("=", "");
        return withoutPad;
    }

    static byte[] decode(String src) {
        Base32 decoder = new Base32();
        byte[] raw = decoder.decode(src);

        if (raw == null || raw.length < 4) {
            throw new IllegalArgumentException("Invalid encoding for source string");
        }

        byte[] crcBytes = Arrays.copyOfRange(raw, raw.length-2, raw.length);
        byte[] dataBytes = Arrays.copyOfRange(raw, 0, raw.length-2);

        int crc = ByteBuffer.wrap(crcBytes).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;
        int actual = crc16(dataBytes);

        if (actual != crc) {
            throw new IllegalArgumentException("CRC is invalid");
        }

        return dataBytes;
    }

    static byte[] decode(Type expectedType, String src) {
        byte[] raw = decode(src);
        byte[] dataBytes = Arrays.copyOfRange(raw, 1, raw.length);
        Type type = NKey.Type.fromPrefix(raw[0] & 0xFF);
        
        if (type != expectedType) {
            throw new IllegalArgumentException("Unexpected type");
        }
    
        return dataBytes;
    }

    static DecodedSeed decodeSeed(String seed) {
        byte[] raw = decode(seed);

        // Need to do the reverse here to get back to internal representation.
        int b1 = raw[0] & 248;                          // 248 = 11111000
        int b2 = (raw[0]&7)<<5 | ((raw[1] & 248) >> 3); // 7 = 00000111
    
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

    static NKey createPair(Type type, SecureRandom random) throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
        if (random == null) {
            random = SecureRandom.getInstance("SHA1PRNG", "SUN");
        } 

        byte[] seed = new byte[NKey.ed25519.getCurve().getField().getb()/8];
        random.nextBytes(seed);

        EdDSAPrivateKeySpec privKeySpec = new EdDSAPrivateKeySpec(seed, NKey.ed25519);
        EdDSAPrivateKey privKey = new EdDSAPrivateKey(privKeySpec);
        EdDSAPublicKeySpec pubKeySpec = new EdDSAPublicKeySpec(privKey.getA(), NKey.ed25519);
        EdDSAPublicKey pubKey = new EdDSAPublicKey(pubKeySpec);
        GroupElement A = pubKey.getA();
        byte[] pubBytes = A.toByteArray();

        byte[] bytes = new byte[pubBytes.length+seed.length];
        System.arraycopy(seed, 0, bytes, 0, seed.length);
        System.arraycopy(pubBytes, 0, bytes, seed.length, pubBytes.length);

        String encoded = encodeSeed(type, bytes);
        return new NKey(type, null, encoded);
    }

    public static NKey createAccount(SecureRandom random) throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
        return createPair(Type.ACCOUNT, random);
    }

    public static NKey createCluster(SecureRandom random) throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
        return createPair(Type.CLUSTER, random);
    }
    
    public static NKey createServer(SecureRandom random) throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
        return createPair(Type.SERVER, random);
    }
    
    public static NKey createUser(SecureRandom random) throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
        return createPair(Type.USER, random);
    }

    public static NKey fromPublicKey(String publicKey) {
        byte[] raw = decode(publicKey);
        int prefix = raw[0] & 0xFF;

        if (!checkValidPublicPrefixByte(prefix)) {
            throw new IllegalArgumentException("Not a valid public NKey");
        }

        Type type = NKey.Type.fromPrefix(prefix);
        return new NKey(type, publicKey, null);
    }

    public static NKey fromSeed(String seed) {
        DecodedSeed decoded = decodeSeed(seed); // Should throw on bad seed
        return new NKey(Type.fromPrefix(decoded.prefix), null, seed);
    }

    /**
     * The seed or private key per the Ed25519 spec, encoded with encodeSeed.
     */
    private String privateKeyAsSeed;

    /**
     * The public key, maybe null. Used for public only NKeys.
     */
    private String publicKey;

    private Type type;

    private NKey(Type t, String publicKey, String privateKey) {
        this.type = t;
        this.privateKeyAsSeed = privateKey;
        this.publicKey = publicKey;
    }

    /**
     * @return the encoded seed for this NKey
     */
    public String getSeed() {
        if (privateKeyAsSeed == null) {
            throw new IllegalStateException("Public-only NKey");
        }
        return privateKeyAsSeed;
    }


    /**
     * @return the encoded public key for this NKey
     * 
     * @throws GeneralSecurityException if there is an encryption problem
     * @throws IOException if there is a problem encoding the public key
     */
    public String getPublicKey() throws GeneralSecurityException, IOException {
        if (publicKey != null) {
            return publicKey;
        }

        KeyPair keys = getKeyPair();
        EdDSAPublicKey pubKey = (EdDSAPublicKey) keys.getPublic();
        GroupElement A = pubKey.getA();
        byte[] pubBytes = A.toByteArray();

        return encode(this.type, pubBytes);
    }


    /**
     * @return the encoded private key for this NKey
     * 
     * @throws GeneralSecurityException if there is an encryption problem
     * @throws IOException if there is a problem encoding the key
     */
    public String getPrivateKey()  throws GeneralSecurityException, IOException{
        KeyPair keys = getKeyPair();
        EdDSAPrivateKey privKey = (EdDSAPrivateKey) keys.getPrivate();
        byte[] bytes = privKey.getEncoded();
        return encode(Type.PRIVATE, bytes);
    }

    /**
     * @return A keypair that represents this NKey in Java security form.
     * 
     * @throws GeneralSecurityException if there is an encryption problem
     * @throws IOException if there is a problem encoding or decoding
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
        EdDSAPublicKeySpec pubKeySpec = new EdDSAPublicKeySpec(new GroupElement(NKey.ed25519.getCurve(), pubBytes), NKey.ed25519);
        EdDSAPublicKey pubKey = new EdDSAPublicKey(pubKeySpec);

        return new KeyPair(pubKey, privKey);
    }

    /**
     * @return get the Type of this NKey
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
     * @throws IOException if there is a problem reading the data
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
     * @param input the bytes that were signed
     * @param signature the bytes for the signature
     * @return true if the signature matches this keys signature for the input.
     * 
     * @throws GeneralSecurityException if there is an encryption problem
     * @throws IOException if there is a problem reading the data
     */
    public boolean verify(byte[] input, byte[] signature) throws GeneralSecurityException, IOException {
        Signature sgr = new EdDSAEngine(MessageDigest.getInstance(NKey.ed25519.getHashAlgorithm()));
        PublicKey sKey = null;
        
        if (privateKeyAsSeed != null) {
            sKey = getKeyPair().getPublic();
        } else {
            String encodedPublicKey = getPublicKey();
            byte[] decodedPublicKey = decode(this.type, encodedPublicKey);
            EdDSAPublicKeySpec pubKeySpec = new EdDSAPublicKeySpec(new GroupElement(NKey.ed25519.getCurve(), decodedPublicKey), NKey.ed25519);
            sKey = new EdDSAPublicKey(pubKeySpec);
        }

        sgr.initVerify(sKey);
        sgr.update(input);

        return sgr.verify(signature);
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof NKey)) {
            return false;
        }

        NKey otherNKey = (NKey) o;

        if (this.type != otherNKey.type) {
            return false;
        }

        if (this.privateKeyAsSeed == null) {
            return this.publicKey.equals(otherNKey.publicKey);
        }
    
        return this.privateKeyAsSeed.equals(otherNKey.privateKeyAsSeed);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.type.prefix;

        if (this.privateKeyAsSeed == null) {
            result = 31 * result + this.publicKey.hashCode();
        } else {
            result = 31 * result + this.privateKeyAsSeed.hashCode();
        }
        return result;
    }

}