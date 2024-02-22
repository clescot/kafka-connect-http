# Authentication

# basic authentication

# digest authentication

# basic authentication with basic proxy authentication

# basic authentication with digest proxy authentication

# digest authentication with basic proxy authentication

# digest authentication with digest proxy authentication

# ssl cert-based authentication

# openID Connect (OIDC) authentication with oAuth2 authorization

## OAuth 2.1

2 flows :

## client credential flow

application is the resource owner, has got id and credentials : machine to machine interactions

how to use it with keycloak : https://fullstackdeveloper.guru/2022/03/16/how-to-set-up-keycloak-for-oauth2-client-credentials-flow/

- client_id
- client_secret
- grant_type : client_credentials

authentication modes : 
- basic (HTTP Header Authorization: Basic $CREDENTIALS) with $CREDENTIALS = base64($client_id:$client_secret) 
  with a POST to $baseUrl/token with an url parameter grant_type=client_credentials
- POST request with form (header "Content-Type: application/x-www-form-urlencoded") with form parameter : 
  - client_id
  - client_secret
  - grant_type=client_credentials
- JWT Bearer Grant Type mechanism => client send a JWT signed with its cryptographic key
JSON Web Key Set are public keys to verify the signature of the Json Web Token (JWT)
parameters to sign JWT : 
- iss : issuer => client_id => or base service url (<baseServiceUrl>/.well-known/openid-configuration)
- sub : subject => client_id
- aud : audience => URL of the Authorization Server's Token Endpoint
- jti : JWT ID => A unique identifier for the token
- exp : expiration time 
- iat : (optional) time at which the token has been issued



## PKCE Authorization code flow

application is a web app, and acting on behalf of the user. application get an access and a refresh token.
=> impersonation


OIDC endpoints : 
- https://..../.well-known/openid-configuration : list all the endpoints
- authorization endpoints permits to authenticate and grant access =>redirect to the token endpoint to get it : described in authorization_endpoint