# Configure authentication

This guide shows how to make the HTTP client authenticate against protected servers:
Basic and Digest credentials, OAuth2 client credentials, proxy authentication, and custom
SSL material.

## Where these settings go

Every authentication setting is part of a **configuration**. A configuration is identified
by an id and all of its settings share the prefix `config.<id>.`:

- the `default` configuration always exists and uses the prefix `config.default.`;
- additional configurations are listed in `config.ids` (comma-separated) and use
  `config.<your-id>.` — for example `config.my-id.httpclient.authentication.basic.username`.

A configuration also has a predicate that decides which requests it applies to. Requests
that match no configuration fall back to `default`. See the
[HTTP Sink reference](../reference/http-sink-connector.md#configuration-settings) for the
predicate settings.

## Basic authentication

```json
{
  "config.default.httpclient.authentication.basic.activate": "true",
  "config.default.httpclient.authentication.basic.username": "my-user",
  "config.default.httpclient.authentication.basic.password": "my-password"
}
```

| Setting | Required | Default |
| --- | --- | --- |
| `config.<id>.httpclient.authentication.basic.activate` | yes | `false` |
| `config.<id>.httpclient.authentication.basic.username` | yes | — |
| `config.<id>.httpclient.authentication.basic.password` | yes | — |
| `config.<id>.httpclient.authentication.basic.charset` | no | `ISO-8859-1` |

## Digest authentication

```json
{
  "config.default.httpclient.authentication.digest.activate": "true",
  "config.default.httpclient.authentication.digest.username": "my-user",
  "config.default.httpclient.authentication.digest.password": "my-password"
}
```

| Setting | Required | Default |
| --- | --- | --- |
| `config.<id>.httpclient.authentication.digest.activate` | yes | `false` |
| `config.<id>.httpclient.authentication.digest.username` | yes | — |
| `config.<id>.httpclient.authentication.digest.password` | yes | — |
| `config.<id>.httpclient.authentication.digest.charset` | no | `US-ASCII` |

The digest algorithm draws randomness from the configuration's random generator, so the
`httpclient.secure.random.*` settings affect it.

## OAuth2 client credentials flow

Use this for machine-to-machine (M2M) calls, where the application itself is the resource
owner. The connector obtains a token from the provider's well-known configuration endpoint
and sends it as a bearer token.

```json
{
  "config.default.httpclient.authentication.oauth2.client.credentials.flow.activate": "true",
  "config.default.httpclient.authentication.oauth2.client.credentials.flow.well.known.url": "https://my-idp/.well-known/openid-configuration",
  "config.default.httpclient.authentication.oauth2.client.credentials.flow.client.authentication.method": "client_secret_basic",
  "config.default.httpclient.authentication.oauth2.client.credentials.flow.client.id": "my-client-id",
  "config.default.httpclient.authentication.oauth2.client.credentials.flow.client.secret": "my-client-secret",
  "config.default.httpclient.authentication.oauth2.client.credentials.flow.scopes": "scope1,scope2"
}
```

| Setting | Required | Default |
| --- | --- | --- |
| `...oauth2.client.credentials.flow.activate` | yes | `false` |
| `...oauth2.client.credentials.flow.well.known.url` | yes | — |
| `...oauth2.client.credentials.flow.client.authentication.method` | no | `client_secret_basic` |
| `...oauth2.client.credentials.flow.client.id` | yes | — |
| `...oauth2.client.credentials.flow.client.secret` | yes | — |
| `...oauth2.client.credentials.flow.client.issuer` | only for JWT | — |
| `...oauth2.client.credentials.flow.client.jws.algorithm` | only for JWT | `HS256` |
| `...oauth2.client.credentials.flow.scopes` | no | — |

Supported client authentication methods: `client_secret_basic`, `client_secret_post`, and
`client_secret_jwt`.

> The connector implements the **client credentials flow** only. Interactive flows such as
> authorization code (PKCE) are not supported, because a Kafka Connect task has no browser
> to redirect.

## Proxy authentication

If your proxy requires its own credentials, configure them separately from the endpoint
credentials:

```json
{
  "config.default.proxy.httpclient.hostname": "my-proxy",
  "config.default.proxy.httpclient.port": "3128",
  "config.default.proxy.httpclient.type": "HTTP",
  "config.default.httpclient.proxy.authentication.basic.activate": "true",
  "config.default.httpclient.proxy.authentication.basic.username": "proxy-user",
  "config.default.httpclient.proxy.authentication.basic.password": "proxy-password"
}
```

Digest proxy credentials use the same shape with `.proxy.authentication.digest.*` and default
to the `US-ASCII` charset.

## Custom SSL material

For servers that use a private certificate authority, provide a trust store. For mutual TLS,
also provide a key store.

```json
{
  "config.default.httpclient.ssl.truststore.path": "/etc/certs/my.truststore.jks",
  "config.default.httpclient.ssl.truststore.password": "changeit",
  "config.default.httpclient.ssl.truststore.type": "jks",
  "config.default.httpclient.ssl.keystore.path": "/etc/certs/my.keystore.jks",
  "config.default.httpclient.ssl.keystore.password": "changeit",
  "config.default.httpclient.ssl.keystore.type": "pkcs12"
}
```

| Setting | Notes |
| --- | --- |
| `config.<id>.httpclient.ssl.keystore.path` / `.password` / `.type` / `.algorithm` | client key material (mutual TLS) |
| `config.<id>.httpclient.ssl.truststore.path` / `.password` / `.type` / `.algorithm` | trusted server certificates |
| `config.<id>.httpclient.ssl.truststore.always.trust` | trust **any** certificate |

> `httpclient.ssl.truststore.always.trust` disables transport security: the client accepts
> any server certificate. Never enable it in production.

You can also relax hostname verification with
`config.<id>.okhttp.ssl.skip.hostname.verification` — likewise unsafe outside tests.

## Next steps

- The connector selects a configuration per request with a predicate:
  [predicate settings](../reference/http-sink-connector.md#configuration-settings).
- Template processors can rewrite headers, including `Authorization`, before publishing:
  [customize exchange templates](customize-exchange-templates.md).
