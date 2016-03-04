package io.nats.client;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NUID {

	final static Logger logger = LoggerFactory.getLogger(NUID.class);

	// NUID needs to be very fast to generate and truly unique, all while being entropy pool friendly.
	// We will use 12 bytes of crypto generated data (entropy draining), and 10 bytes of sequential data
	// that is started at a pseudo random number and increments with a pseudo-random increment.
	// Total is 22 bytes of base 36 ascii text :)
	
	// Constants
	static final char[] digits 	= { '0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z' };
	static final int 	base		= 36;
	static final int 	preLen		= 12;
	static final int 	seqLen		= 10;
	static final long 	maxPre		= 4738381338321616896L;	// base^preLen == 36^12
	static final long 	maxSeq		= 3656158440062976L; 	// base^seqLen == 36^10
	static final long	minInc		= 33L;
	static final long	maxInc		= 333L;
	static final int 	totalLen 	= preLen + seqLen;
	static Random srand;
	static Random prand;

	// Instance fields
	char[] pre;
	long seq;
	long inc;
	

	// Global NUID
	public static NUID globalNUID = new NUID();
	private static Object lock = new Object();

	static NUID getInstance() {
		if (globalNUID == null) {
			globalNUID = new NUID();
		}
		return globalNUID;
	}
	
	public NUID () {
		if (srand == null) {
			try {
				srand = SecureRandom.getInstance("SHA1PRNG");
			} catch (NoSuchAlgorithmException e) {
				logger.error("stan: nuid algorithm not found", e);
			}
			prand = new Random(); 
		}
		seq = nextLong(prand, maxSeq);
		inc = minInc + nextLong(prand, maxInc-minInc);
		pre = new char[preLen];
		for (int i = 0; i < preLen; i++) {
			pre[i] = '0';
		}
		randomizePrefix();
	}
	
	// Generate the next NUID string from the global locked NUID instance.
	public static String nextGlobal() {
		synchronized(lock) {
			return getInstance().next();
		}
	}
	
	// Generate the next NUID string.
	public String next() {
		// Increment and capture.
		seq += inc;
		if (seq >= maxSeq) {
			randomizePrefix();
			resetSequential();
		}

		// Copy prefix
		char[] b = new char[totalLen];
		System.arraycopy(pre, 0, b, 0, preLen);
		
		// copy in the seq in base36.
		int i = b.length;
		for (long l = seq; i > preLen; l /= base) {
			i--;
			b[i] = digits[(int)(l%base)];
		}
		return new String(b);
	}

	// Resets the sequntial portion of the NUID
	void resetSequential() {
		seq = nextLong(prand, maxSeq);
		inc = minInc + nextLong(prand, maxInc-minInc);
	}
	
	// Generate a new prefix from random.
	// This will drain entropy and will be called automatically when we exhaust the sequential
	// Will panic if it gets an error from rand.Int()
	public void randomizePrefix() {
		long n = nextLong(srand, maxPre);
		int i = pre.length;
		for (long l = n; i>0; l /= base) {
			i--;
			pre[i] = digits[(int)(l % base)];
		}
	}

	static long nextLong(Random rng, long n) {
		// error checking and 2^x checking removed for simplicity.
		long bits, val;
		do {
			bits = (rng.nextLong() << 1) >>> 1;
			val = bits % n;
		} while (bits-val+(n-1) < 0L);
		return val;
	}

	/**
	 * @return the pre
	 */
	char[] getPre() {
		return pre;
	}

	/**
	 * @param pre the pre to set
	 */
	void setPre(char[] pre) {
		this.pre = pre;
	}

	/**
	 * @return the seq
	 */
	long getSeq() {
		return seq;
	}

	/**
	 * @param seq the seq to set
	 */
	void setSeq(long seq) {
		this.seq = seq;
	}

	/**
	 * @return the inc
	 */
	long getInc() {
		return inc;
	}

	/**
	 * @param inc the inc to set
	 */
	void setInc(long inc) {
		this.inc = inc;
	}
}
