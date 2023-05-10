package io.nats.client.support;

import io.nats.client.NKey;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.nats.client.support.JwtUtils.issueUserJWT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JwtUtilsTests {

    @Test
    public void issueUserJWTSuccessMinimal() throws Exception {
        NKey userKey = NKey.fromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY".toCharArray());
        NKey signingKey = NKey.fromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU".toCharArray());
        String accountId = "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6";
        String jwt = issueUserJWT(signingKey, accountId, new String(userKey.getPublicKey()), null, null, null, 1633043378);
        String cred = String.format(JwtUtils.NATS_USER_JWT_FORMAT, jwt, new String(userKey.getSeed()));
        /*
            Generated JWT:
            {
                "iat": 1633043378,
                "jti": "X4TWD4I3YGCAZ4PRKULOOUACR5W5AIKSM6MXVPJZBLXHZZMNFZAQ",
                "iss": "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY",
                "name": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
                "nats": {
                    "issuer_account": "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6",
                    "type": "user",
                    "version": 2
                },
                "sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ"
            }
         */
        assertEquals("-----BEGIN NATS USER JWT-----\n" +
            "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJpYXQiOjE2MzMwNDMzNzgsImp0aSI6Ilg0VFdENEkzWUdDQVo0UFJLVUxPT1VBQ1I1VzVBSUtTTTZNWFZQSlpCTFhIWlpNTkZaQVEiLCJpc3MiOiJBRFE0QllNNUtJQ1I1T1hEU1AzUzNXVko1Q1lFT1JHUUtUNzJTVlJGMlpEVkE3TFRGS01DSVBHWSIsIm5hbWUiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiIsIm5hdHMiOnsiaXNzdWVyX2FjY291bnQiOiJBQ1haUkFMSUwyMldSRVREUlhZS09ZREI3WEMzRTdNQlNWVVNVTUZBQ082T001VlBSTkZNT09PNiIsInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6Mn0sInN1YiI6IlVBNktPTVE2N1hPRTNGSEUzN1c0T1hBRFZYVllJU0JOTFRCVVQyTFNZNVZGS0FJSjdDUkRSMlJaIn0.CUo-0aCxS1DvPpvZ3cZMXmLlQRMKYRfQjjXSFiCkmRrIJKX9crdkcIBuoYIgfczt5oTEDyR1ldS_ucipxCnoAQ\n" +
            "------END NATS USER JWT------\n" +
            "\n" +
            "************************* IMPORTANT *************************\n" +
            "    NKEY Seed printed below can be used to sign and prove identity.\n" +
            "    NKEYs are sensitive and should be treated as secrets.\n" +
            "\n" +
            "-----BEGIN USER NKEY SEED-----\n" +
            "SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY\n" +
            "------END USER NKEY SEED------\n" +
            "\n" +
            "*************************************************************\n",
            cred);
    }

    @Test
    public void issueUserJWTSuccessAllArgs() throws Exception {
        NKey userKey = NKey.fromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY".toCharArray());
        NKey signingKey = NKey.fromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU".toCharArray());
        String accountId = "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6";
        String jwt = issueUserJWT(signingKey, accountId, new String(userKey.getPublicKey()), "name", Duration.ofSeconds(100), new String[]{"tag1", "tag\\two"}, 1633043378);
        String cred = String.format(JwtUtils.NATS_USER_JWT_FORMAT, jwt, new String(userKey.getSeed()));
        /*
            Generated JWT:
            {
                "exp": 1633043478,
                "iat": 1633043378,
                "jti": "43FXG2FKK7OY2QH7OJGYOD7BF2UFU447EDJZEPO3VSBNJJVAKJCA",
                "iss": "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY",
                "name": "name",
                "nats": {
                    "issuer_account": "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6",
                    "tags": [
                        "tag1",
                        "tag\\two"
                    ],
                    "type": "user",
                    "version": 2
                },
                "sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ"
            }
        */
        assertEquals("-----BEGIN NATS USER JWT-----\n" +
            "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJleHAiOjE2MzMwNDM0NzgsImlhdCI6MTYzMzA0MzM3OCwianRpIjoiNDNGWEcyRktLN09ZMlFIN09KR1lPRDdCRjJVRlU0NDdFREpaRVBPM1ZTQk5KSlZBS0pDQSIsImlzcyI6IkFEUTRCWU01S0lDUjVPWERTUDNTM1dWSjVDWUVPUkdRS1Q3MlNWUkYyWkRWQTdMVEZLTUNJUEdZIiwibmFtZSI6Im5hbWUiLCJuYXRzIjp7Imlzc3Vlcl9hY2NvdW50IjoiQUNYWlJBTElMMjJXUkVURFJYWUtPWURCN1hDM0U3TUJTVlVTVU1GQUNPNk9NNVZQUk5GTU9PTzYiLCJ0YWdzIjpbInRhZzEiLCJ0YWdcXHR3byJdLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9LCJzdWIiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiJ9.NdCdguIm89sg07PIlwdjkWesjHzKNQ2XYUd_t_Pfz7BFNz3gNbdcm5L-4mzvzH4l7NMI2Hbp0I7v_P26zU5cCQ\n" +
            "------END NATS USER JWT------\n" +
            "\n" +
            "************************* IMPORTANT *************************\n" +
            "    NKEY Seed printed below can be used to sign and prove identity.\n" +
            "    NKEYs are sensitive and should be treated as secrets.\n" +
            "\n" +
            "-----BEGIN USER NKEY SEED-----\n" +
            "SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY\n" +
            "------END USER NKEY SEED------\n" +
            "\n" +
            "*************************************************************\n",
            cred);
    }

    @Test
    public void issueUserJWTSuccessCustom() throws Exception {
        NKey userKey = NKey.fromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY".toCharArray());
        NKey signingKey = NKey.fromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU".toCharArray());

        JwtUtils.UserClaim userClaim = new JwtUtils.UserClaim("ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6")
            .pub(new JwtUtils.Permission()
                .allow(new String[] {"pub-allow-subject"})
                .deny(new String[] {"pub-deny-subject"}))
            .sub(new JwtUtils.Permission()
                .allow(new String[] {"sub-allow-subject"})
                .deny(new String[] {"sub-deny-subject"}))
            .tags(new String[]{"tag1", "tag\\two"});

        String jwt = issueUserJWT(signingKey, new String(userKey.getPublicKey()), null, null, 1633043378, userClaim);
        String cred = String.format(JwtUtils.NATS_USER_JWT_FORMAT, jwt, new String(userKey.getSeed()));

        /*
            Generated JWT:
            {
                "iat": 1633043378,
                "jti": "JUB65ANDEIROWEGEBOVRGEJRW6FX74PIJX3LJDYHBKKQITDGATTA",
                "iss": "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY",
                "name": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
                "nats": {
                    "issuer_account": "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6",
                    "tags": ["tag1", "tag\\two"],
                    "type": "user",
                    "version": 2,
                    "pub": {
                        "allow": ["pub-allow-subject"],
                        "deny": ["pub-deny-subject"]
                    },
                    "sub": {
                        "allow": ["sub-allow-subject"],
                        "deny": ["sub-deny-subject"]
                    }
                },
                "sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ"
            }
         */

        assertEquals("-----BEGIN NATS USER JWT-----\n" +
                "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJpYXQiOjE2MzMwNDMzNzgsImp0aSI6IkpVQjY1QU5ERUlST1dFR0VCT1ZSR0VKUlc2Rlg3NFBJSlgzTEpEWUhCS0tRSVRER0FUVEEiLCJpc3MiOiJBRFE0QllNNUtJQ1I1T1hEU1AzUzNXVko1Q1lFT1JHUUtUNzJTVlJGMlpEVkE3TFRGS01DSVBHWSIsIm5hbWUiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiIsIm5hdHMiOnsiaXNzdWVyX2FjY291bnQiOiJBQ1haUkFMSUwyMldSRVREUlhZS09ZREI3WEMzRTdNQlNWVVNVTUZBQ082T001VlBSTkZNT09PNiIsInRhZ3MiOlsidGFnMSIsInRhZ1xcdHdvIl0sInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6MiwicHViIjp7ImFsbG93IjpbInB1Yi1hbGxvdy1zdWJqZWN0Il0sImRlbnkiOlsicHViLWRlbnktc3ViamVjdCJdfSwic3ViIjp7ImFsbG93IjpbInN1Yi1hbGxvdy1zdWJqZWN0Il0sImRlbnkiOlsic3ViLWRlbnktc3ViamVjdCJdfX0sInN1YiI6IlVBNktPTVE2N1hPRTNGSEUzN1c0T1hBRFZYVllJU0JOTFRCVVQyTFNZNVZGS0FJSjdDUkRSMlJaIn0.bZAJPqeoBuwKYZHaWGNJlyejPmSRxhM_2lMlu6fx4UxsjUdMkJJLl1P5KpGl95cyjnxpShDz7aanGgtAPfnzDQ\n" +
                "------END NATS USER JWT------\n" +
                "\n" +
                "************************* IMPORTANT *************************\n" +
                "    NKEY Seed printed below can be used to sign and prove identity.\n" +
                "    NKEYs are sensitive and should be treated as secrets.\n" +
                "\n" +
                "-----BEGIN USER NKEY SEED-----\n" +
                "SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY\n" +
                "------END USER NKEY SEED------\n" +
                "\n" +
                "*************************************************************\n",
            cred);
    }

    @Test
    public void issueUserJWTBadSigningKey() throws Exception {
        NKey userKey = NKey.fromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY".toCharArray());
        // should be account, but this is a user key:
        NKey signingKey = NKey.fromSeed("SUAIW7IZ2YDQYLTE4FJ64ZBX7UMLCN57V6GHALKMUSMJCU5PJDNUO6BVUI".toCharArray());
        String accountId = "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6";
        assertThrows(IllegalArgumentException.class, () -> issueUserJWT(signingKey, accountId, new String(userKey.getPublicKey()), null, null, null, 1633043378));
    }

    @Test
    public void issueUserJWTBadAccountId() throws Exception {
        NKey userKey = NKey.fromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY".toCharArray());
        NKey signingKey = NKey.fromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU".toCharArray());
        // should be account, but this is a user key:
        String accountId = "UDN6WZFPYTS4YSUHUD4YFFU5NVKT6BVCY5QXQFYF3I23AER622SBOVUZ";
        assertThrows(IllegalArgumentException.class, () -> issueUserJWT(signingKey, accountId, new String(userKey.getPublicKey()), null, null, null, 1633043378));
    }

    @Test
    public void issueUserJWTBadPublicUserKey() throws Exception {
        NKey userKey = NKey.fromSeed("SAADFHQTEKYBOCG4CPEPNAJ5FLRX4G4WTCNTAIOKN3LARLHGVKB4BRUHYY".toCharArray());
        NKey signingKey = NKey.fromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU".toCharArray());
        String accountId = "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6";
        assertThrows(IllegalArgumentException.class, () -> issueUserJWT(signingKey, accountId, new String(userKey.getPublicKey()), null, null, null, 1633043378));
    }
}
