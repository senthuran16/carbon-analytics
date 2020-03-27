package org.wso2.carbon.streaming.integrator.core.extensions.installer.beans;

import org.wso2.carbon.config.annotation.Configuration;

@Configuration(namespace = "extension.installation", description = "Configuration for installation of extensions")
public class ExtensionsInstallerConfigurations {
    private boolean enabled = false;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
