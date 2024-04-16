/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

/**
 * Settings for remote cluster state at one place.
 *
 * @opensearch.internal
 */
public class RemoteClusterStateSettings {

    public static final TimeValue SLOW_WRITE_LOGGING_THRESHOLD_DEFAULT = TimeValue.timeValueMillis(10000);

    public static final TimeValue INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final TimeValue GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final TimeValue METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    /**
     * Used to specify if cluster state metadata should be published to remote store
     */
    public static final Setting<Boolean> REMOTE_CLUSTER_STATE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.state.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    public static final Setting<TimeValue> SLOW_WRITE_LOGGING_THRESHOLD_SETTING = Setting.timeSetting(
        "gateway.slow_write_logging_threshold",
        SLOW_WRITE_LOGGING_THRESHOLD_DEFAULT,
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> INDEX_METADATA_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.index_metadata.upload_timeout",
        INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.global_metadata.upload_timeout",
        GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.metadata_manifest.upload_timeout",
        METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile TimeValue slowWriteLoggingThreshold;
    private volatile TimeValue indexMetadataUploadTimeout;
    private volatile TimeValue globalMetadataUploadTimeout;
    private volatile TimeValue metadataManifestUploadTimeout;

    public RemoteClusterStateSettings(ClusterSettings clusterSettings) {
        slowWriteLoggingThreshold = clusterSettings.get(SLOW_WRITE_LOGGING_THRESHOLD_SETTING);
        indexMetadataUploadTimeout = clusterSettings.get(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING);
        globalMetadataUploadTimeout = clusterSettings.get(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING);
        metadataManifestUploadTimeout = clusterSettings.get(METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING);
        clusterSettings.addSettingsUpdateConsumer(SLOW_WRITE_LOGGING_THRESHOLD_SETTING, this::setSlowWriteLoggingThreshold);
        clusterSettings.addSettingsUpdateConsumer(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING, this::setIndexMetadataUploadTimeout);
        clusterSettings.addSettingsUpdateConsumer(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING, this::setGlobalMetadataUploadTimeout);
        clusterSettings.addSettingsUpdateConsumer(METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING, this::setMetadataManifestUploadTimeout);
    }

    public TimeValue getSlowWriteLoggingThreshold() {
        return slowWriteLoggingThreshold;
    }

    public TimeValue getIndexMetadataUploadTimeout() {
        return indexMetadataUploadTimeout;
    }

    public TimeValue getGlobalMetadataUploadTimeout() {
        return globalMetadataUploadTimeout;
    }

    public TimeValue getMetadataManifestUploadTimeout() {
        return metadataManifestUploadTimeout;
    }

    private void setSlowWriteLoggingThreshold(TimeValue slowWriteLoggingThreshold) {
        this.slowWriteLoggingThreshold = slowWriteLoggingThreshold;
    }

    private void setIndexMetadataUploadTimeout(TimeValue indexMetadataUploadTimeout) {
        this.indexMetadataUploadTimeout = indexMetadataUploadTimeout;
    }

    private void setGlobalMetadataUploadTimeout(TimeValue globalMetadataUploadTimeout) {
        this.globalMetadataUploadTimeout = globalMetadataUploadTimeout;
    }

    private void setMetadataManifestUploadTimeout(TimeValue metadataManifestUploadTimeout) {
        this.metadataManifestUploadTimeout = metadataManifestUploadTimeout;
    }
}
