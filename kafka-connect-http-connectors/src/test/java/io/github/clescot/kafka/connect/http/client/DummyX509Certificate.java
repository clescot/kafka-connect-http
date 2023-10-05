package io.github.clescot.kafka.connect.http.client;

import javax.security.auth.x500.X500Principal;
import java.math.BigInteger;
import java.security.*;
import java.security.cert.*;
import java.util.Date;
import java.util.Set;

public class DummyX509Certificate extends X509Certificate {

    int version;
    BigInteger serialNumber = BigInteger.valueOf(123456789L);
    private X500Principal issuerDN = new X500Principal("CN=Issuer, OU=JavaSoft, O=Sun Microsystems, C=US");
    private X500Principal subjectDN = new X500Principal("CN=Duke, OU=JavaSoft, O=Sun Microsystems, C=US");
    private Date notBefore = new Date();
    private Date notAfter = new Date();
    private String sigAlgName;
    private String sigAlgOID;

    @Override
    public void checkValidity() throws CertificateExpiredException, CertificateNotYetValidException {

    }

    @Override
    public void checkValidity(Date date) throws CertificateExpiredException, CertificateNotYetValidException {

    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public BigInteger getSerialNumber() {
        return serialNumber;
    }

    @Override
    public Principal getIssuerDN() {
        return issuerDN;
    }

    @Override
    public Principal getSubjectDN() {
        return subjectDN;
    }

    @Override
    public Date getNotBefore() {
        return notBefore;
    }

    @Override
    public Date getNotAfter() {
        return notAfter;
    }

    @Override
    public byte[] getTBSCertificate() {
        return new byte[0];
    }

    @Override
    public byte[] getSignature() {
        return new byte[0];
    }

    @Override
    public String getSigAlgName() {
        return sigAlgName;
    }

    @Override
    public String getSigAlgOID() {
        return sigAlgOID;
    }

    @Override
    public byte[] getSigAlgParams() {
        return new byte[0];
    }

    @Override
    public boolean[] getIssuerUniqueID() {
        return new boolean[0];
    }

    @Override
    public boolean[] getSubjectUniqueID() {
        return new boolean[0];
    }

    @Override
    public boolean[] getKeyUsage() {
        return new boolean[0];
    }

    @Override
    public int getBasicConstraints() {
        return 0;
    }

    @Override
    public byte[] getEncoded() throws CertificateEncodingException {
        return new byte[0];
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void setSerialNumber(BigInteger serialNumber) {
        this.serialNumber = serialNumber;
    }

    public void setIssuerDN(X500Principal issuerDN) {
        this.issuerDN = issuerDN;
    }

    public void setSubjectDN(X500Principal subjectDN) {
        this.subjectDN = subjectDN;
    }

    public void setNotBefore(Date notBefore) {
        this.notBefore = notBefore;
    }

    public void setNotAfter(Date notAfter) {
        this.notAfter = notAfter;
    }

    public void setSigAlgName(String sigAlgName) {
        this.sigAlgName = sigAlgName;
    }

    public void setSigAlgOID(String sigAlgOID) {
        this.sigAlgOID = sigAlgOID;
    }

    @Override
    public void verify(PublicKey key) throws CertificateException, NoSuchAlgorithmException, InvalidKeyException, NoSuchProviderException, SignatureException {

    }

    @Override
    public void verify(PublicKey key, String sigProvider) throws CertificateException, NoSuchAlgorithmException, InvalidKeyException, NoSuchProviderException, SignatureException {

    }

    @Override
    public PublicKey getPublicKey() {
        return null;
    }

    @Override
    public boolean hasUnsupportedCriticalExtension() {
        return false;
    }

    @Override
    public Set<String> getCriticalExtensionOIDs() {
        return null;
    }

    @Override
    public Set<String> getNonCriticalExtensionOIDs() {
        return null;
    }

    @Override
    public byte[] getExtensionValue(String oid) {
        return new byte[0];
    }

    @Override
    public String toString() {
        return "DummyX509Certificate{" +
                "version=" + version +
                ", serialNumber=" + serialNumber +
                ", issuerDN=" + issuerDN +
                ", subjectDN=" + subjectDN +
                ", notBefore=" + notBefore +
                ", notAfter=" + notAfter +
                ", sigAlgName='" + sigAlgName + '\'' +
                ", sigAlgOID='" + sigAlgOID + '\'' +
                '}';
    }
}
