package io.github.clescot.kafka.connect.http.client.okhttp;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.client.HttpException;
import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.HttpUrl;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.CookieManager;
import java.net.HttpCookie;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OkHttpCookieJar implements CookieJar {


    public static final String COOKIE = "Cookie";
    public static final String SET_COOKIE = "Set-Cookie";
    private final CookieManager cookieManager;

    public OkHttpCookieJar(CookieManager cookieManager) {
        this.cookieManager = cookieManager;
    }

    @Override
    public void saveFromResponse(HttpUrl url, List<Cookie> cookies) {
        try {
            URI uri = url.uri();
            Map<String, List<String>> headers = new HashMap<>();
            List<String> cookieHeaders = new ArrayList<>();

            for (Cookie cookie : cookies) {
                HttpCookie httpCookie = new HttpCookie(cookie.name(), cookie.value());
                httpCookie.setDomain(cookie.domain());
                httpCookie.setPath(cookie.path());
                httpCookie.setSecure(cookie.secure());
                httpCookie.setHttpOnly(cookie.httpOnly());
                httpCookie.setMaxAge(cookie.expiresAt());
                if (cookie.persistent()) {
                    //convert milliseconds from okhttp cookie to seconds for java.net Cookie
                    long maxAge = (cookie.expiresAt() - System.currentTimeMillis()) / 1000;
                    // maxAge in seconds
                    httpCookie.setMaxAge(maxAge);
                }
                //some attributes are not supported by okhttp's Cookie: version, comment, commentURL, portlist

                cookieHeaders.add(httpCookie.toString());
            }

            headers.put(SET_COOKIE, cookieHeaders);
            cookieManager.put(uri, headers);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public List<Cookie> loadForRequest(@NotNull HttpUrl url) {
        try {
            URI uri = url.uri();
            Map<String, List<String>> headers = cookieManager.get(uri, new HashMap<>());
            List<String> cookieHeaders = headers.get(COOKIE);
            if (cookieHeaders == null || cookieHeaders.isEmpty()) {
                return new ArrayList<>();
            }

            List<Cookie> cookies = new ArrayList<>();
            for (String cookieHeader : cookieHeaders) {
                String[] cookiePairs = cookieHeader.split("; ");
                for (String pair : cookiePairs) {
                    String[] nameValue = pair.split("=", 2);
                    if (nameValue.length == 2) {
                        Cookie.Builder builder = new Cookie.Builder();
                        builder.name(nameValue[0])
                                .value(nameValue[1])
                                .domain(url.host())
                                .path("/");
                        if(url.isHttps()){
                            builder.secure();
                        }
                        if(url.host().endsWith("localhost") ||
                                //url is an ip address
                                url.host().matches("\\d+\\.\\d+\\.\\d+\\.\\d+")){
                            builder.hostOnlyDomain(url.host());
                        }
                        cookies.add(builder.build());
                    }
                }
            }
            return cookies;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}