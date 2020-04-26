/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.siddhi.extensions.installer.cli;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Handles responses of requests, made by the {@link RequestHandler}.
 */
public class ResponseHandler {

    private static final Logger logger = LoggerFactory.getLogger(ResponseHandler.class);

    private static final String EXTENSION_INFO = "extensionInfo";
    private static final String NAME = "name";
    private static final String DISPLAY_NAME = "displayName";
    private static final String VERSION = "version";
    private static final String DOWNLOAD = "download";
    private static final String INSTRUCTIONS = "instructions";
    private static final String EXTENSION_STATUS = "extensionStatus";
    private static final String MANUALLY_INSTALL = "manuallyInstall";
    private static final String STATUS = "status";
    private static final String FAILED = "failed";
    private static final String DOES_SHARE_DEPENDENCIES = "doesShareDependencies";
    private static final String SHARES_WITH = "sharesWith";
    private static final String EXTENSION_ROW_FORMAT = "%-50s%-30s%-20s";

    private ResponseHandler() {
        // Prevents Instantiation.
    }

    /**
     * Parses the given response to JSON, and returns it as a {@link JsonElement}.
     *
     * @param response Response string, that was obtained from a request.
     * @return Parsed response.
     * @throws ExtensionsInstallerCliException The given response can not be parsed to JSON.
     */
    private static JsonElement parseResponse(String response) throws ExtensionsInstallerCliException {
        try {
            return new JsonParser().parse(response);
        } catch (JsonSyntaxException e) {
            /*
            During erroneous cases such as requesting status of an extension whose configuration does not exist,
            the response will be a string (error message), not a JSON object.
            We identify such a response by attempting to parse that as a JSON, and failing.
            */
            throw new ExtensionsInstallerCliException(response, e);
        }
    }

    /**
     * Notifies installation statuses of all extensions, from the given response.
     *
     * @param response The response received by the corresponding request.
     * @throws ExtensionsInstallerCliException Failed to parse the given response.
     */
    public static void handleAllExtensionStatusesResponse(String response) throws ExtensionsInstallerCliException {
        JsonElement parsedResponse = parseResponse(response);
        if (parsedResponse instanceof JsonObject) {
            StringBuilder message = new StringBuilder();
            message.append(System.lineSeparator());
            message.append(" (*) - Contains manually installable dependencies");
            message.append(System.lineSeparator());
            message.append(getExtensionRowHeader());
            message.append(System.lineSeparator());
            // Gather each extension's details.
            for (Map.Entry<String, JsonElement> extension : ((JsonObject) parsedResponse).entrySet()) {
                String name = extension.getKey();
                String displayName = ((JsonObject) extension.getValue()).get(EXTENSION_INFO).getAsJsonObject()
                    .get(DISPLAY_NAME).getAsString();
                String version = ((JsonObject) extension.getValue()).get(EXTENSION_INFO).getAsJsonObject()
                    .get(VERSION).getAsString();
                String status = ((JsonObject) extension.getValue()).get(EXTENSION_STATUS).getAsString();
                // Append the row which represents an extension's status.
                message.append(getExtensionRow(name, displayName, version, status,
                    ((JsonObject) extension.getValue()).keySet().contains(MANUALLY_INSTALL)));
                message.append(System.lineSeparator());
            }
            logger.info(message.toString());
        }
    }

    /**
     * Notifies installation status of the extension with the given name, from the given response.
     *
     * @param response      The response received by the corresponding request.
     * @param extensionName Name of the extension of which, installation status is requested.
     * @throws ExtensionsInstallerCliException Failed to parse the given response.
     */
    public static void handleExtensionStatusResponse(String response, String extensionName)
        throws ExtensionsInstallerCliException {
        JsonElement parsedResponse = parseResponse(response);
        if (parsedResponse instanceof JsonObject) {
            String displayName = ((JsonObject) parsedResponse).get(EXTENSION_INFO).getAsJsonObject()
                .get(DISPLAY_NAME).getAsString();
            String version = ((JsonObject) parsedResponse).get(EXTENSION_INFO).getAsJsonObject()
                .get(VERSION).getAsString();
            String status = ((JsonObject) parsedResponse).get(EXTENSION_STATUS).getAsString();
            StringBuilder message = new StringBuilder();
            message.append(System.lineSeparator());
            message.append(getExtensionRowHeader());
            message.append(System.lineSeparator());
            // Get information of manually installable dependencies (if any).
            String manuallyInstall = getManuallyInstallMessage((JsonObject) parsedResponse);
            message.append(getExtensionRow(extensionName, displayName, version, status, manuallyInstall != null));
            if (manuallyInstall != null) {
                message.append(System.lineSeparator());
                message.append(manuallyInstall);
            }
            logger.info(message.toString());
        }
    }

    private static String getExtensionRowHeader() {
        return String.format(EXTENSION_ROW_FORMAT, "EXTENSION", "NAME", "STATUS");
    }

    private static String getExtensionRow(String extensionName, String displayName, String version, String status,
                                          boolean hasManuallyInstall) {
        String formattedDisplayName = String.format("%s %s", displayName, version);
        return String.format(EXTENSION_ROW_FORMAT,
            formattedDisplayName, extensionName, hasManuallyInstall ? status + " (*)" : status);
    }

    /**
     * Notifies information about an installation, from the given response.
     *
     * @param response The response received by the corresponding request.
     * @throws ExtensionsInstallerCliException Failed to parse the given response.
     */
    public static void handleInstallationResponse(String response) throws ExtensionsInstallerCliException {
        JsonElement parsedResponse = parseResponse(response);
        if (parsedResponse instanceof JsonObject) {
            StringBuilder message = new StringBuilder();
            message.append("Installation completed with status: ");
            message.append(((JsonObject) parsedResponse).get(STATUS).getAsString());
            message.append(".");
            message.append(System.lineSeparator());
            // Get information of dependencies of which, installation has been failed (if any).
            String failureOccurredMessage = getFailureOccurredMessage((JsonObject) parsedResponse);
            if (failureOccurredMessage != null) {
                message.append(System.lineSeparator());
                message.append(failureOccurredMessage);
            }
            // Get information of manually installable dependencies (if any).
            String manuallyInstallMessage = getManuallyInstallMessage((JsonObject) parsedResponse);
            if (manuallyInstallMessage != null) {
                message.append(System.lineSeparator());
                message.append(manuallyInstallMessage);
            }
            message.append(System.lineSeparator());
            logger.info(message.toString());
            logger.warn("Please restart the server.");
        }
    }

    private static String getManuallyInstallMessage(JsonObject parsedResponse) {
        if (parsedResponse.get(MANUALLY_INSTALL) != null) {
            StringBuilder message = new StringBuilder("The following dependencies should be manually installed: ");
            message.append(System.lineSeparator());
            message.append(System.lineSeparator());
            // Gather each dependency's details.
            for (JsonElement dependency : parsedResponse.getAsJsonArray(MANUALLY_INSTALL)) {
                // Dependency information.
                message.append("  - ");
                message.append(dependency.getAsJsonObject().get(NAME).getAsString());
                message.append(" ");
                message.append(dependency.getAsJsonObject().get(VERSION).getAsString());
                message.append(":");
                message.append(System.lineSeparator());
                // Instructions.
                String instructions =
                    dependency.getAsJsonObject().getAsJsonObject(DOWNLOAD).get(INSTRUCTIONS).getAsString();
                instructions = instructions.replace("<br/>", System.lineSeparator());
                message.append(instructions);
                message.append(System.lineSeparator());
                message.append(System.lineSeparator());
            }
            return message.toString();
        }
        return null;
    }

    private static String getFailureOccurredMessage(JsonObject parsedResponse) {
        if (parsedResponse.get(FAILED) != null) {
            StringBuilder message = new StringBuilder("Failure occurred with the following dependencies:");
            message.append(System.lineSeparator());

            // Gather each failed dependency's information.
            for (JsonElement failedDependency : parsedResponse.getAsJsonArray(FAILED)) {
                message.append(" ");
                message.append(failedDependency.getAsJsonObject().get(NAME).getAsString());
                message.append(" ");
                message.append(failedDependency.getAsJsonObject().get(VERSION).getAsString());
                message.append(System.lineSeparator());
            }
            return message.toString();
        }
        return null;
    }

    /**
     * Checks whether an extension shares dependencies with other extensions, from the given response.
     *
     * @param response The response received by the corresponding request.
     * @return Whether the extension shares dependencies with other extensions.
     * @throws ExtensionsInstallerCliException Failed to parse the given response.
     */
    public static boolean isDependencySharingExtensionsAvailable(String response)
        throws ExtensionsInstallerCliException {
        JsonElement parsedResponse = parseResponse(response);
        if (parsedResponse instanceof JsonObject &&
            ((JsonObject) parsedResponse).get(DOES_SHARE_DEPENDENCIES) != null &&
            ((JsonObject) parsedResponse).get(DOES_SHARE_DEPENDENCIES).getAsBoolean()) {
            String message = "The extension shares its dependencies with the following extensions: " +
                String.join(", ", ((JsonObject) parsedResponse).get(SHARES_WITH).getAsJsonObject().keySet());
            logger.warn(message);
            return true;
        }
        return false;
    }

    /**
     * Notifies information about an un-installation, from the given response.
     *
     * @param response The response received by the corresponding request.
     * @throws ExtensionsInstallerCliException Failed to parse the given response.
     */
    public static void handleUnInstallationResponse(String response) throws ExtensionsInstallerCliException {
        JsonElement parsedResponse = parseResponse(response);
        if (parsedResponse instanceof JsonObject) {
            StringBuilder message = new StringBuilder();
            message.append("Un-installation completed with status: ");
            message.append(((JsonObject) parsedResponse).get(STATUS).getAsString());
            message.append(".");
            message.append(System.lineSeparator());
            // Get information of dependencies of which, un-installation has been failed (if any).
            String failureOccurredMessage = getFailureOccurredMessage((JsonObject) parsedResponse);
            if (failureOccurredMessage != null) {
                message.append(System.lineSeparator());
                message.append(failureOccurredMessage);
            }
            message.append(System.lineSeparator());
            logger.info(message.toString());
            logger.warn("Please restart the server.");
        }
    }
}
