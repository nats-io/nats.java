package io.nats.client.support;

import io.nats.client.NKey;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.JwtUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JwtUtilsTests {

    @Test
    public void issueUserJWTSuccessMinimal() throws Exception {
        NKey userKey = NKey.fromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY".toCharArray());
        NKey signingKey = NKey.fromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU".toCharArray());
        String accountId = "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6";
        String jwt = issueUserJWT(signingKey, accountId, new String(userKey.getPublicKey()), null, null, null, 1633043378);
        String cred = String.format(NATS_USER_JWT_FORMAT, jwt, new String(userKey.getSeed()));
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
                    "version": 2,
                    "subs": -1,
                    "data": -1,
                    "payload": -1
                },
                "sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ"
            }
         */
        assertEquals("-----BEGIN NATS USER JWT-----\n" +
            "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJpYXQiOjE2MzMwNDMzNzgsImp0aSI6IlROUUhFRzVMQllNVzVJM0lMVFBYSDRON0dHSEVGM0lLTURNSDRRSjROUUFVSUQ1MkZFM1EiLCJpc3MiOiJBRFE0QllNNUtJQ1I1T1hEU1AzUzNXVko1Q1lFT1JHUUtUNzJTVlJGMlpEVkE3TFRGS01DSVBHWSIsIm5hbWUiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiIsIm5hdHMiOnsiaXNzdWVyX2FjY291bnQiOiJBQ1haUkFMSUwyMldSRVREUlhZS09ZREI3WEMzRTdNQlNWVVNVTUZBQ082T001VlBSTkZNT09PNiIsInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6Miwic3VicyI6LTEsImRhdGEiOi0xLCJwYXlsb2FkIjotMX0sInN1YiI6IlVBNktPTVE2N1hPRTNGSEUzN1c0T1hBRFZYVllJU0JOTFRCVVQyTFNZNVZGS0FJSjdDUkRSMlJaIn0.9uzP8We3PCdR6G_50kvaKxLIOIwTlnc2eZe5249XFQj1JGgsHC0VJ3MSMerxIdNpWbSPQrgy1oKL2yU-xjr8Bw\n" +
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
        String cred = String.format(NATS_USER_JWT_FORMAT, jwt, new String(userKey.getSeed()));
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
                    "version": 2,
                    "subs": -1,
                    "data": -1,
                    "payload": -1
                },
                "sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ"
            }
        */
        assertEquals("-----BEGIN NATS USER JWT-----\n" +
            "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJleHAiOjE2MzMwNDM0NzgsImlhdCI6MTYzMzA0MzM3OCwianRpIjoiWk1ENEJCRkxTVklZVzNBRVNHVk5UNDdEQ1dSN0lPV1paUVk3NFBLQTNDWEhQV0NYMkFOUSIsImlzcyI6IkFEUTRCWU01S0lDUjVPWERTUDNTM1dWSjVDWUVPUkdRS1Q3MlNWUkYyWkRWQTdMVEZLTUNJUEdZIiwibmFtZSI6Im5hbWUiLCJuYXRzIjp7Imlzc3Vlcl9hY2NvdW50IjoiQUNYWlJBTElMMjJXUkVURFJYWUtPWURCN1hDM0U3TUJTVlVTVU1GQUNPNk9NNVZQUk5GTU9PTzYiLCJ0YWdzIjpbInRhZzEiLCJ0YWdcXHR3byJdLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjIsInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTF9LCJzdWIiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiJ9.5cUsiR0BTT1g6mbLeNRROvEfHFIG3m22Zt9q_xjozjEj_JREZsZHm84noE0Ec8xWN92bIrETmaX-qywOGBdcCg\n" +
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

        UserClaim userClaim = new UserClaim("ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6")
            .pub(new Permission()
                .allow(new String[] {"pub-allow-subject"})
                .deny(new String[] {"pub-deny-subject"}))
            .sub(new Permission()
                .allow(new String[] {"sub-allow-subject"})
                .deny(new String[] {"sub-deny-subject"}))
            .tags(new String[]{"tag1", "tag\\two"});

        String jwt = issueUserJWT(signingKey, new String(userKey.getPublicKey()), null, null, 1633043378, userClaim);
        String cred = String.format(NATS_USER_JWT_FORMAT, jwt, new String(userKey.getSeed()));

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
                    },
                    "subs": -1,
                    "data": -1,
                    "payload": -1
                },
                "sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ"
            }
         */

        assertEquals("-----BEGIN NATS USER JWT-----\n" +
                "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJpYXQiOjE2MzMwNDMzNzgsImp0aSI6IldGMkJDR0VMRVVOTzJDWEYzUjZaN0w0RTNKS09PM0tCREVDRU5IUkVSSTczRzYzRUtZVlEiLCJpc3MiOiJBRFE0QllNNUtJQ1I1T1hEU1AzUzNXVko1Q1lFT1JHUUtUNzJTVlJGMlpEVkE3TFRGS01DSVBHWSIsIm5hbWUiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiIsIm5hdHMiOnsiaXNzdWVyX2FjY291bnQiOiJBQ1haUkFMSUwyMldSRVREUlhZS09ZREI3WEMzRTdNQlNWVVNVTUZBQ082T001VlBSTkZNT09PNiIsInRhZ3MiOlsidGFnMSIsInRhZ1xcdHdvIl0sInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6MiwicHViIjp7ImFsbG93IjpbInB1Yi1hbGxvdy1zdWJqZWN0Il0sImRlbnkiOlsicHViLWRlbnktc3ViamVjdCJdfSwic3ViIjp7ImFsbG93IjpbInN1Yi1hbGxvdy1zdWJqZWN0Il0sImRlbnkiOlsic3ViLWRlbnktc3ViamVjdCJdfSwic3VicyI6LTEsImRhdGEiOi0xLCJwYXlsb2FkIjotMX0sInN1YiI6IlVBNktPTVE2N1hPRTNGSEUzN1c0T1hBRFZYVllJU0JOTFRCVVQyTFNZNVZGS0FJSjdDUkRSMlJaIn0.kKExDG8HH5zWytrknE8XXKSnkUdWQYi3FgCfNks0IM9LlJyDbiQIadpJa4eyFh_ajCIXKGhMTKIULEwNbrPbAQ\n" +
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
    public void issueUserJWTSuccessCustomLimits() throws Exception {
        NKey userKey = NKey.fromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY".toCharArray());
        NKey signingKey = NKey.fromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU".toCharArray());

        UserClaim userClaim = new UserClaim("ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6")
                .subs(1)
                .data(2)
                .payload(3);

        String jwt = issueUserJWT(signingKey, new String(userKey.getPublicKey()), null, null, 1633043378, userClaim);
        String cred = String.format(NATS_USER_JWT_FORMAT, jwt, new String(userKey.getSeed()));

        /*
            Generated JWT:
            {
                "iat": 1633043378,
                "jti": "JUB65ANDEIROWEGEBOVRGEJRW6FX74PIJX3LJDYHBKKQITDGATTA",
                "iss": "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY",
                "name": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
                "nats": {
                    "issuer_account": "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6",
                    "type": "user",
                    "version": 2,
                    "subs": 1,
                    "data": 2,
                    "payload": 3
                },
                "sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ"
            }
        */

        assertEquals("-----BEGIN NATS USER JWT-----\n" +
                "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJpYXQiOjE2MzMwNDMzNzgsImp0aSI6IkVLNUpHR0VYVEhKWUZZM0VYSEZXSVVQTE5HWU82QjRaSUZETk43V1k2NVFYVEhMT0JPVUEiLCJpc3MiOiJBRFE0QllNNUtJQ1I1T1hEU1AzUzNXVko1Q1lFT1JHUUtUNzJTVlJGMlpEVkE3TFRGS01DSVBHWSIsIm5hbWUiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiIsIm5hdHMiOnsiaXNzdWVyX2FjY291bnQiOiJBQ1haUkFMSUwyMldSRVREUlhZS09ZREI3WEMzRTdNQlNWVVNVTUZBQ082T001VlBSTkZNT09PNiIsInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6Miwic3VicyI6MSwiZGF0YSI6MiwicGF5bG9hZCI6M30sInN1YiI6IlVBNktPTVE2N1hPRTNGSEUzN1c0T1hBRFZYVllJU0JOTFRCVVQyTFNZNVZGS0FJSjdDUkRSMlJaIn0.7nMNv_ugIM28U9va9cQR64LwbQZYEz7DZqJnD7Z-h49ZXp0PB96bHXfvzwgBgxNuTvlnWpnBmt5XtF33PlRMDw\n" +
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

    @Test
    public void userJwt() throws Exception {
        UserClaim uc = new UserClaim("test-issuer-account");
        assertEquals(DEFAULT_JSON, uc.toJson());
        System.out.println(DEFAULT_JSON);

        List<TimeRange> times = new ArrayList<>();
        times.add(new TimeRange("01:15:00", "03:15:00"));

        uc.tags("tag1", "tag2")
            .pub(new Permission().allow("pa1", "pa2").deny("pd1", "pd2"))
            .sub(new Permission().allow("sa1", "sa2").deny("sd1", "sd2"))
            .resp(new ResponsePermission().maxMsgs(99).expires(999))
            .src("src1", "src2")
            .times(times)
            .locale("US/Eastern")
            .subs(42)
            .data(43)
            .payload(44)
            .bearerToken(true)
            .allowedConnectionTypes("nats", "tls");
        assertEquals(FULL_JSON, uc.toJson());
    }

    private static final String DEFAULT_JSON = "{\"issuer_account\":\"test-issuer-account\",\"type\":\"user\",\"version\":2,\"subs\":-1,\"data\":-1,\"payload\":-1}";
    private static final String FULL_JSON = "{\"issuer_account\":\"test-issuer-account\",\"tags\":[\"tag1\",\"tag2\"],\"type\":\"user\",\"version\":2,\"pub\":{\"allow\":[\"pa1\",\"pa2\"],\"deny\":[\"pd1\",\"pd2\"]},\"sub\":{\"allow\":[\"sa1\",\"sa2\"],\"deny\":[\"sd1\",\"sd2\"]},\"resp\":{\"max\":99,\"ttl\":999000000},\"src\":[\"src1\",\"src2\"],\"times\":[{\"start\":\"01:15:00\",\"end\":\"03:15:00\"}],\"times_location\":\"US\\/Eastern\",\"subs\":42,\"data\":43,\"payload\":44,\"bearer_token\":true,\"allowed_connection_types\":[\"nats\",\"tls\"]}";

    /*
        DEFAULT_JSON
        {
            "issuer_account": "test-issuer-account",
            "type": "user",
            "version": 2,
            "subs": -1,
            "data": -1,
            "payload": -1
        }

        FULL_JSON
        {
            "issuer_account": "test-issuer-account",
            "tags": ["tag1", "tag2"],
            "type": "user",
            "version": 2,
            "pub": {
                "allow": ["pa1", "pa2"],
                "deny": ["pd1", "pd2"]
            },
            "sub": {
                "allow": ["sa1", "sa2"],
                "deny": ["sd1", "sd2"]
            },
            "resp": {
                "max": 99,
                "ttl": 999000000
            },
            "src": ["src1", "src2"],
            "times": [{
                "start": "01:15:00",
                "end": "03:15:00"
            }],
            "times_location": "US\/Eastern",
            "subs": 42,
            "data": 43,
            "payload": 44,
            "bearer_token": true,
            "allowed_connection_types": ["nats", "tls"]
        }
    */
}
