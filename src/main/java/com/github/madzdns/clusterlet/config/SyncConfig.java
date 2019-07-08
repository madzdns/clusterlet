package com.github.madzdns.clusterlet.config;

public class SyncConfig {

    private String clusterStorageConfigPath,
            keyStorePath, trustStorePath,
            keyStorePassword, trustStorePassword,
            keyStorePassword2nd,
            certificatePath;

    public SyncConfig(String clusterStorageConfigPath,
                      String keyStorePath,
                      String trustStorePath, String keyStorePassword,
                      String trustStorePassword, String keyStorePassword2nd, String certificatePath) {

        this.clusterStorageConfigPath = clusterStorageConfigPath;
        this.keyStorePath = keyStorePath;
        this.trustStorePath = trustStorePath;
        this.keyStorePassword = keyStorePassword;
        this.trustStorePassword = trustStorePassword;
        this.keyStorePassword2nd = keyStorePassword2nd;
        this.certificatePath = certificatePath;
    }

    public String getClusterStorageConfigPath() {
        return clusterStorageConfigPath;
    }

    public String getKeyStorePath() {
        return keyStorePath;
    }

    public String getTrustStorePath() {
        return trustStorePath;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public String getKeyStorePassword2nd() {
        return keyStorePassword2nd;
    }

    public String getCertificatePath() {
        return certificatePath;
    }
}
