# missing features

## bodyType
Only the bodyType set to 'STRING' (most usual http requests), is supported.
'BYTE_ARRAY' and 'MULTIPART' will be supported in future releases.


## authentication is not yet supported

we use actually the Async HTTP Client library, which has got great performances, but seems dormant.
We plan to switch to the okhttp library from square, and rely on it to add some authentication features (oauth2).