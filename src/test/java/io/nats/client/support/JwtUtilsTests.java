// Copyright 2023 The NATS Authors
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

import io.nats.client.NKey;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.JwtUtils.*;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class JwtUtilsTests {
    static NKey USER_KEY = NKey.fromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY".toCharArray());
    static NKey SIGNING_KEY = NKey.fromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU".toCharArray());
    static String ACCOUNT_ID = "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6";

    @Test
    public void issueUserJWTSuccessMinimal() throws Exception {
        String expectedClaimBody = "{\"aud\":\"audience\",\"jti\":\"PASRIRDVH2NWAPOKCO7TFIJVWI2OESTOH4CJ2PSGYH77YPQRXPVA\",\"iat\":1633043378,\"iss\":\"ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY\",\"name\":\"UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ\",\"sub\":\"UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ\",\"nats\":{\"issuer_account\":\"ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6\",\"type\":\"user\",\"version\":2,\"subs\":-1,\"data\":-1,\"payload\":-1}}";
        String expectedCred = "-----BEGIN NATS USER JWT-----\n" +
            "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJhdWQiOiJhdWRpZW5jZSIsImp0aSI6IlBBU1JJUkRWSDJOV0FQT0tDTzdURklKVldJMk9FU1RPSDRDSjJQU0dZSDc3WVBRUlhQVkEiLCJpYXQiOjE2MzMwNDMzNzgsImlzcyI6IkFEUTRCWU01S0lDUjVPWERTUDNTM1dWSjVDWUVPUkdRS1Q3MlNWUkYyWkRWQTdMVEZLTUNJUEdZIiwibmFtZSI6IlVBNktPTVE2N1hPRTNGSEUzN1c0T1hBRFZYVllJU0JOTFRCVVQyTFNZNVZGS0FJSjdDUkRSMlJaIiwic3ViIjoiVUE2S09NUTY3WE9FM0ZIRTM3VzRPWEFEVlhWWUlTQk5MVEJVVDJMU1k1VkZLQUlKN0NSRFIyUloiLCJuYXRzIjp7Imlzc3Vlcl9hY2NvdW50IjoiQUNYWlJBTElMMjJXUkVURFJYWUtPWURCN1hDM0U3TUJTVlVTVU1GQUNPNk9NNVZQUk5GTU9PTzYiLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjIsInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTF9fQ.nt_bErX7UuqDSxC8NUORaB0r4IS_33Wds1vV_o0HRI-BwE9UxM-zAFtq43o3-d98s6u1jASgVXp0h81om8mVDw\n" +
            "------END NATS USER JWT------\n" +
            "\n" +
            "************************* IMPORTANT *************************\n" +
            "NKEY Seed printed below can be used to sign and prove identity.\n" +
            "NKEYs are sensitive and should be treated as secrets.\n" +
            "\n" +
            "-----BEGIN USER NKEY SEED-----\n" +
            "SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY\n" +
            "------END USER NKEY SEED------\n" +
            "\n" +
            "*************************************************************\n";

        String jwt = issueUserJWT(SIGNING_KEY, ACCOUNT_ID, new String(USER_KEY.getPublicKey()), null, null, null, 1633043378, "audience");
        String claimBody = getClaimBody(jwt);
        String cred = String.format(NATS_USER_JWT_FORMAT, jwt, new String(USER_KEY.getSeed()));
        /*
            Formatted Claim Body:
            {
                "aud": "audience",
                "jti": "PASRIRDVH2NWAPOKCO7TFIJVWI2OESTOH4CJ2PSGYH77YPQRXPVA",
                "iat": 1633043378,
                "iss": "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY",
                "name": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
                "sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
                "nats": {
                    "issuer_account": "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6",
                    "type": "user",
                    "version": 2,
                    "subs": -1,
                    "data": -1,
                    "payload": -1
                }
            }
         */
        assertEquals(expectedClaimBody, claimBody);
        assertEquals(expectedCred, cred);
    }

    @Test
    public void issueUserJWTSuccessMinimalCoverageNonAudienceApi() throws Exception {
        String expectedClaimBody = "{\"jti\":\"WQYMEEISPFLDXLNAOEF3TC2FWLZCKVCPQZWMDBDCGT3ZTSSHSBYA\",\"iat\":1633043378,\"iss\":\"ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY\",\"name\":\"UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ\",\"sub\":\"UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ\",\"nats\":{\"issuer_account\":\"ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6\",\"type\":\"user\",\"version\":2,\"subs\":-1,\"data\":-1,\"payload\":-1}}";
        String expectedCred = "-----BEGIN NATS USER JWT-----\n" +
            "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJqdGkiOiJXUVlNRUVJU1BGTERYTE5BT0VGM1RDMkZXTFpDS1ZDUFFaV01EQkRDR1QzWlRTU0hTQllBIiwiaWF0IjoxNjMzMDQzMzc4LCJpc3MiOiJBRFE0QllNNUtJQ1I1T1hEU1AzUzNXVko1Q1lFT1JHUUtUNzJTVlJGMlpEVkE3TFRGS01DSVBHWSIsIm5hbWUiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiIsInN1YiI6IlVBNktPTVE2N1hPRTNGSEUzN1c0T1hBRFZYVllJU0JOTFRCVVQyTFNZNVZGS0FJSjdDUkRSMlJaIiwibmF0cyI6eyJpc3N1ZXJfYWNjb3VudCI6IkFDWFpSQUxJTDIyV1JFVERSWFlLT1lEQjdYQzNFN01CU1ZVU1VNRkFDTzZPTTVWUFJORk1PT082IiwidHlwZSI6InVzZXIiLCJ2ZXJzaW9uIjoyLCJzdWJzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xfX0.5Zq3wTy_5WeBYmFmYjI1OArCJ8IiJpFCflm5tnoWB7bYOOWoINR2t8a0i2fuGVYtFmDBAV8im_G2sibtlu53AQ\n" +
            "------END NATS USER JWT------\n" +
            "\n" +
            "************************* IMPORTANT *************************\n" +
            "NKEY Seed printed below can be used to sign and prove identity.\n" +
            "NKEYs are sensitive and should be treated as secrets.\n" +
            "\n" +
            "-----BEGIN USER NKEY SEED-----\n" +
            "SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY\n" +
            "------END USER NKEY SEED------\n" +
            "\n" +
            "*************************************************************\n";

        String jwt = issueUserJWT(SIGNING_KEY, ACCOUNT_ID, new String(USER_KEY.getPublicKey()), null, null, null, 1633043378, null);
        _testMinimalNoAudience(expectedClaimBody, expectedCred, USER_KEY, jwt);

        jwt = issueUserJWT(SIGNING_KEY, ACCOUNT_ID, new String(USER_KEY.getPublicKey()), null, null, null, 1633043378);
        _testMinimalNoAudience(expectedClaimBody, expectedCred, USER_KEY, jwt);
    }

    private static void _testMinimalNoAudience(String expectedClaimBody, String expectedCred, NKey userKey, String jwt) {
        String claimBody = getClaimBody(jwt);
        String cred = String.format(NATS_USER_JWT_FORMAT, jwt, new String(userKey.getSeed()));
        /*
            Formatted Claim Body:
            {
            	"jti": "WQYMEEISPFLDXLNAOEF3TC2FWLZCKVCPQZWMDBDCGT3ZTSSHSBYA",
            	"iat": 1633043378,
            	"iss": "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY",
            	"name": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
            	"sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
            	"nats": {
            		"issuer_account": "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6",
            		"type": "user",
            		"version": 2,
            		"subs": -1,
            		"data": -1,
            		"payload": -1
            	}
            }
        */
        assertEquals(expectedClaimBody, claimBody);
        assertEquals(expectedCred, cred);
    }

    @Test
    public void issueUserJWTSuccessAllArgs() throws Exception {
        String jwt = issueUserJWT(SIGNING_KEY, ACCOUNT_ID, new String(USER_KEY.getPublicKey()), "name", Duration.ofSeconds(100), new String[]{"tag1", "tag\\two"}, 1633043378, "audience");
        String claimBody = getClaimBody(jwt);
        String cred = String.format(NATS_USER_JWT_FORMAT, jwt, new String(USER_KEY.getSeed()));
        /*
            Formatted Claim Body:
            {
                "aud": "audience",
                "jti": "YK4IE2OAVVGD7CZ2OHGVDSQCRIHL4HNRPAQ2DE5B6VQ6HM5ZFWYA",
                "iat": 1633043378,
                "iss": "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY",
                "name": "name",
                "sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
                "exp": 1633043478,
                "nats": {
                    "issuer_account": "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6",
                    "tags": ["tag1", "tag\\two"],
                    "type": "user",
                    "version": 2,
                    "subs": -1,
                    "data": -1,
                    "payload": -1
                }
            }
        */
        String expectedClaimBody = "{\"aud\":\"audience\",\"jti\":\"YK4IE2OAVVGD7CZ2OHGVDSQCRIHL4HNRPAQ2DE5B6VQ6HM5ZFWYA\",\"iat\":1633043378,\"iss\":\"ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY\",\"name\":\"name\",\"sub\":\"UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ\",\"exp\":1633043478,\"nats\":{\"issuer_account\":\"ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6\",\"tags\":[\"tag1\",\"tag\\\\two\"],\"type\":\"user\",\"version\":2,\"subs\":-1,\"data\":-1,\"payload\":-1}}";
        String expectedCred = "-----BEGIN NATS USER JWT-----\n" +
            "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJhdWQiOiJhdWRpZW5jZSIsImp0aSI6IllLNElFMk9BVlZHRDdDWjJPSEdWRFNRQ1JJSEw0SE5SUEFRMkRFNUI2VlE2SE01WkZXWUEiLCJpYXQiOjE2MzMwNDMzNzgsImlzcyI6IkFEUTRCWU01S0lDUjVPWERTUDNTM1dWSjVDWUVPUkdRS1Q3MlNWUkYyWkRWQTdMVEZLTUNJUEdZIiwibmFtZSI6Im5hbWUiLCJzdWIiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiIsImV4cCI6MTYzMzA0MzQ3OCwibmF0cyI6eyJpc3N1ZXJfYWNjb3VudCI6IkFDWFpSQUxJTDIyV1JFVERSWFlLT1lEQjdYQzNFN01CU1ZVU1VNRkFDTzZPTTVWUFJORk1PT082IiwidGFncyI6WyJ0YWcxIiwidGFnXFx0d28iXSwidHlwZSI6InVzZXIiLCJ2ZXJzaW9uIjoyLCJzdWJzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xfX0.cX9OENTYC54mV5g3q1rL9QJiazhBnI88poJ-ssYwDE4ywpi4WhAinXXYd7wXaaTqks97_9cY25XP01sFTqUeDQ\n" +
            "------END NATS USER JWT------\n" +
            "\n" +
            "************************* IMPORTANT *************************\n" +
            "NKEY Seed printed below can be used to sign and prove identity.\n" +
            "NKEYs are sensitive and should be treated as secrets.\n" +
            "\n" +
            "-----BEGIN USER NKEY SEED-----\n" +
            "SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY\n" +
            "------END USER NKEY SEED------\n" +
            "\n" +
            "*************************************************************\n";
        assertEquals(expectedClaimBody, claimBody);
        assertEquals(expectedCred, cred);
    }

    @Test
    public void issueUserJWTSuccessCustom() throws Exception {
        UserClaim userClaim = new UserClaim("ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6")
            .pub(new Permission()
                .allow("pub-allow-subject")
                .deny("pub-deny-subject"))
            .sub(new Permission()
                .allow("sub-allow-subject")
                .deny("sub-deny-subject"))
            .tags("tag1", "tag\\two");

        String jwt = issueUserJWT(SIGNING_KEY, new String(USER_KEY.getPublicKey()), "custom", null, 1633043378, userClaim);
        String claimBody = getClaimBody(jwt);
        String cred = String.format(NATS_USER_JWT_FORMAT, jwt, new String(USER_KEY.getSeed()));
        /*
            Formatted Claim Body:
            {
                "jti": "XAI5HUYQESXRZIYCDCWRSCXRDLVUG2G75QD4NJAOSTD72B6OIX6Q",
                "iat": 1633043378,
                "iss": "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY",
                "name": "custom",
                "sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
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
                }
            }
         */

        String expectedClaimBody = "{\"jti\":\"XAI5HUYQESXRZIYCDCWRSCXRDLVUG2G75QD4NJAOSTD72B6OIX6Q\",\"iat\":1633043378,\"iss\":\"ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY\",\"name\":\"custom\",\"sub\":\"UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ\",\"nats\":{\"issuer_account\":\"ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6\",\"tags\":[\"tag1\",\"tag\\\\two\"],\"type\":\"user\",\"version\":2,\"pub\":{\"allow\":[\"pub-allow-subject\"],\"deny\":[\"pub-deny-subject\"]},\"sub\":{\"allow\":[\"sub-allow-subject\"],\"deny\":[\"sub-deny-subject\"]},\"subs\":-1,\"data\":-1,\"payload\":-1}}";
        String expectedCred = "-----BEGIN NATS USER JWT-----\n" +
                "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJqdGkiOiJYQUk1SFVZUUVTWFJaSVlDRENXUlNDWFJETFZVRzJHNzVRRDROSkFPU1RENzJCNk9JWDZRIiwiaWF0IjoxNjMzMDQzMzc4LCJpc3MiOiJBRFE0QllNNUtJQ1I1T1hEU1AzUzNXVko1Q1lFT1JHUUtUNzJTVlJGMlpEVkE3TFRGS01DSVBHWSIsIm5hbWUiOiJjdXN0b20iLCJzdWIiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiIsIm5hdHMiOnsiaXNzdWVyX2FjY291bnQiOiJBQ1haUkFMSUwyMldSRVREUlhZS09ZREI3WEMzRTdNQlNWVVNVTUZBQ082T001VlBSTkZNT09PNiIsInRhZ3MiOlsidGFnMSIsInRhZ1xcdHdvIl0sInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6MiwicHViIjp7ImFsbG93IjpbInB1Yi1hbGxvdy1zdWJqZWN0Il0sImRlbnkiOlsicHViLWRlbnktc3ViamVjdCJdfSwic3ViIjp7ImFsbG93IjpbInN1Yi1hbGxvdy1zdWJqZWN0Il0sImRlbnkiOlsic3ViLWRlbnktc3ViamVjdCJdfSwic3VicyI6LTEsImRhdGEiOi0xLCJwYXlsb2FkIjotMX19.hcNv6KKQh6cy6nTu6jOtXgf5vvhkuo3P2cB9s-AeOnYW9BZKWxljKmHHlrPiYJ-YdWvPA1y7XsMeKckRDP1rAQ\n" +
                "------END NATS USER JWT------\n" +
                "\n" +
                "************************* IMPORTANT *************************\n" +
                "NKEY Seed printed below can be used to sign and prove identity.\n" +
                "NKEYs are sensitive and should be treated as secrets.\n" +
                "\n" +
                "-----BEGIN USER NKEY SEED-----\n" +
                "SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY\n" +
                "------END USER NKEY SEED------\n" +
                "\n" +
                "*************************************************************\n";
        assertEquals(expectedClaimBody, claimBody);
        assertEquals(expectedCred, cred);
    }

    @Test
    public void issueUserJWTSuccessCustomLimits() throws Exception {
        UserClaim userClaim = new UserClaim("ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6")
                .subs(1)
                .data(2)
                .payload(3);

        String jwt = issueUserJWT(SIGNING_KEY, new String(USER_KEY.getPublicKey()), null, null, 1633043378, userClaim);
        String claimBody = getClaimBody(jwt);
        String cred = String.format(NATS_USER_JWT_FORMAT, jwt, new String(USER_KEY.getSeed()));

        /*
            Formatted Claim Body:
            {
                "jti": "2GIBM5DP442KSBGE7DAS6ZDAB6Z5OPSHD4WBSHBZI7JVFVYBUMBQ",
                "iat": 1633043378,
                "iss": "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY",
                "name": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
                "sub": "UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ",
                "nats": {
                    "issuer_account": "ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6",
                    "type": "user",
                    "version": 2,
                    "subs": 1,
                    "data": 2,
                    "payload": 3
                }
            }
        */
        String expectedClaimBody = "{\"jti\":\"2GIBM5DP442KSBGE7DAS6ZDAB6Z5OPSHD4WBSHBZI7JVFVYBUMBQ\",\"iat\":1633043378,\"iss\":\"ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY\",\"name\":\"UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ\",\"sub\":\"UA6KOMQ67XOE3FHE37W4OXADVXVYISBNLTBUT2LSY5VFKAIJ7CRDR2RZ\",\"nats\":{\"issuer_account\":\"ACXZRALIL22WRETDRXYKOYDB7XC3E7MBSVUSUMFACO6OM5VPRNFMOOO6\",\"type\":\"user\",\"version\":2,\"subs\":1,\"data\":2,\"payload\":3}}";
        String expectedCred = "-----BEGIN NATS USER JWT-----\n" +
            "eyJ0eXAiOiJKV1QiLCAiYWxnIjoiZWQyNTUxOS1ua2V5In0.eyJqdGkiOiIyR0lCTTVEUDQ0MktTQkdFN0RBUzZaREFCNlo1T1BTSEQ0V0JTSEJaSTdKVkZWWUJVTUJRIiwiaWF0IjoxNjMzMDQzMzc4LCJpc3MiOiJBRFE0QllNNUtJQ1I1T1hEU1AzUzNXVko1Q1lFT1JHUUtUNzJTVlJGMlpEVkE3TFRGS01DSVBHWSIsIm5hbWUiOiJVQTZLT01RNjdYT0UzRkhFMzdXNE9YQURWWFZZSVNCTkxUQlVUMkxTWTVWRktBSUo3Q1JEUjJSWiIsInN1YiI6IlVBNktPTVE2N1hPRTNGSEUzN1c0T1hBRFZYVllJU0JOTFRCVVQyTFNZNVZGS0FJSjdDUkRSMlJaIiwibmF0cyI6eyJpc3N1ZXJfYWNjb3VudCI6IkFDWFpSQUxJTDIyV1JFVERSWFlLT1lEQjdYQzNFN01CU1ZVU1VNRkFDTzZPTTVWUFJORk1PT082IiwidHlwZSI6InVzZXIiLCJ2ZXJzaW9uIjoyLCJzdWJzIjoxLCJkYXRhIjoyLCJwYXlsb2FkIjozfX0.1DbLJayUdxCNaBapTRV8NkPmTGxa5R5TW5QsOB9iZoTuhgcVVI2tCOx1VIn7ccNJvmERw3dOpIf7uPVtxgv2AQ\n" +
            "------END NATS USER JWT------\n" +
            "\n" +
            "************************* IMPORTANT *************************\n" +
            "NKEY Seed printed below can be used to sign and prove identity.\n" +
            "NKEYs are sensitive and should be treated as secrets.\n" +
            "\n" +
            "-----BEGIN USER NKEY SEED-----\n" +
            "SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY\n" +
            "------END USER NKEY SEED------\n" +
            "\n" +
            "*************************************************************\n";
        assertEquals(expectedClaimBody, claimBody);
        assertEquals(expectedCred, cred);
    }

    @Test
    public void issueUserJWTBadSigningKey() {
        NKey userKey = NKey.fromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY".toCharArray());
        // should be account, but this is a user key:
        NKey signingKey = NKey.fromSeed("SUAIW7IZ2YDQYLTE4FJ64ZBX7UMLCN57V6GHALKMUSMJCU5PJDNUO6BVUI".toCharArray());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> issueUserJWT(signingKey, ACCOUNT_ID, new String(userKey.getPublicKey()), null, null, null, 1633043378));
        assertEquals("issueUserJWT requires an account key for the signingKey parameter, but got USER", e.getMessage());
    }

    @Test
    public void issueUserJWTBadAccountId() {
        NKey userKey = NKey.fromSeed("SUAGL3KX4ZBBD53BNNLSHGAAGCMXSEYZ6NTYUBUCPZQGHYNK3ZRQBUDPRY".toCharArray());
        NKey signingKey = NKey.fromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU".toCharArray());
        // should be account, but this is a user key:
        String accountId = "UDN6WZFPYTS4YSUHUD4YFFU5NVKT6BVCY5QXQFYF3I23AER622SBOVUZ";
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> issueUserJWT(signingKey, accountId, new String(userKey.getPublicKey()), null, null, null, 1633043378));
        assertEquals("issueUserJWT requires an account key for the accountId parameter, but got USER", e.getMessage());
    }

    @Test
    public void issueUserJWTBadPublicUserKey() {
        NKey userKey = NKey.fromSeed("SAADFHQTEKYBOCG4CPEPNAJ5FLRX4G4WTCNTAIOKN3LARLHGVKB4BRUHYY".toCharArray());
        NKey signingKey = NKey.fromSeed("SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU".toCharArray());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> issueUserJWT(signingKey, ACCOUNT_ID, new String(userKey.getPublicKey()), null, null, null, 1633043378));
        assertEquals("issueUserJWT requires a user key for the publicUserKey parameter, but got ACCOUNT", e.getMessage());
    }

    @Test
    public void testUserClaimJson() {
        UserClaim uc = new UserClaim("test-issuer-account");
        assertEquals(BASIC_JSON, uc.toJson());

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

    @Test
    public void testMiscCoverage() {
        long seconds = JwtUtils.currentTimeSeconds();
        sleep(1000);
        assertTrue(JwtUtils.currentTimeSeconds() > seconds);
    }

    private static final String BASIC_JSON = "{\"issuer_account\":\"test-issuer-account\",\"type\":\"user\",\"version\":2,\"subs\":-1,\"data\":-1,\"payload\":-1}";
    private static final String FULL_JSON = "{\"issuer_account\":\"test-issuer-account\",\"tags\":[\"tag1\",\"tag2\"],\"type\":\"user\",\"version\":2,\"pub\":{\"allow\":[\"pa1\",\"pa2\"],\"deny\":[\"pd1\",\"pd2\"]},\"sub\":{\"allow\":[\"sa1\",\"sa2\"],\"deny\":[\"sd1\",\"sd2\"]},\"resp\":{\"max\":99,\"ttl\":999000000},\"src\":[\"src1\",\"src2\"],\"times\":[{\"start\":\"01:15:00\",\"end\":\"03:15:00\"}],\"times_location\":\"US\\/Eastern\",\"subs\":42,\"data\":43,\"payload\":44,\"bearer_token\":true,\"allowed_connection_types\":[\"nats\",\"tls\"]}";

    /*
        BASIC_JSON
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
