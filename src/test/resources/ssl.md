# SSL truststore generation

## jks

```bash
keytool -noprompt -keystore client_truststore.jks -storetype jks  -genkey -alias client -keypass Secret123! -storepass Secret123! -keyalg RSA -keysize 2048 -validity 100000 -dname "CN=it.mycorp.com, OU=IT, O=myCorp, L=HappyTown,  C=FI"
```

## pkcs12

```bash
keytool -noprompt -keystore client_truststore.p12 -storetype pkcs12  -genkey -alias client -keypass Secret123! -storepass Secret123! -keyalg RSA -keysize 2048 -validity 100000 -dname "CN=it.mycorp.com, OU=IT, O=myCorp, L=HappyTown,  C=FI"
```
