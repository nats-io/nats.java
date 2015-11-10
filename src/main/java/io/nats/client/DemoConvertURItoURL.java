package io.nats.client;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class DemoConvertURItoURL {

	public static void main(String[] args) {
        URI uri = null;
        URL url = null;
        String uriString = "nats://derek:foobar@localhost:4222";
        // Create a URI object
        try {
            uri = new URI(uriString);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
 
        System.out.println("scheme=" + uri.getScheme() + " host=" + uri.getHost() + " port=" + uri.getPort() + " user=" + uri.getUserInfo());
        // Convert the absolute URI to a URL object
        try {
            url = uri.toURL();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
 
        //print the URI and URL 
        System.out.println("Original URI  : " + uri);
        System.out.println("Converted URL : " + url);
 
	}

}
