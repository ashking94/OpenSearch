/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

/**
 * Factory for a filesystem directory
 *
 * @opensearch.internal
 */
public class FsDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    private static final Logger logger = LogManager.getLogger(FsDirectoryFactory.class);

    public static final Setting<LockFactory> INDEX_LOCK_FACTOR_SETTING = new Setting<>("index.store.fs.fs_lock", "native", (s) -> {
        switch (s) {
            case "native":
                return NativeFSLockFactory.INSTANCE;
            case "simple":
                return SimpleFSLockFactory.INSTANCE;
            default:
                throw new IllegalArgumentException("unrecognized [index.store.fs.fs_lock] \"" + s + "\": must be native or simple");
        } // can we set on both - node and index level, some nodes might be running on NFS so they might need simple rather than native
    }, Property.IndexScope, Property.NodeScope);

    /**
     * Setting to enable block fetch from primary shard to replica shard for remote store index
     */
    public static final Setting<Boolean> PRIMARY_BLOCK_FETCH = Setting.boolSetting(
        "index.store.primary_block_fetch.enabled",
        true,
        Property.IndexScope,
        Property.Dynamic
    );

    /**
     * Default block size for fetching blocks from primary
     */
    public static final Setting<Integer> PRIMARY_BLOCK_SIZE_SETTING = Setting.intSetting(
        "index.store.primary_block_fetch.block_size",
        64 * 1024, // Default 64KB
        4 * 1024,  // Min 4KB
        16 * 1024 * 1024, // Max 16MB
        Property.IndexScope,
        Property.Dynamic
    );

    private final Optional<ClusterService> clusterService;
    private final Optional<Supplier<TransportService>> transportService;

    /**
     * Default constructor used by plugins extending this class
     */
    public FsDirectoryFactory() {
        this(Optional.empty(), Optional.empty());
    }

    public FsDirectoryFactory(Optional<ClusterService> clusterService, Optional<Supplier<TransportService>> transportService) {
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        final Path location = path.resolveIndex();
        final LockFactory lockFactory = indexSettings.getValue(INDEX_LOCK_FACTOR_SETTING);
        Files.createDirectories(location);
        return newFSDirectory(location, lockFactory, indexSettings);
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path, ShardRouting shardRouting) throws IOException {
        final Path location = path.resolveIndex();
        final LockFactory lockFactory = indexSettings.getValue(INDEX_LOCK_FACTOR_SETTING);
        Files.createDirectories(location);
        Directory baseDirectory = newFSDirectory(location, lockFactory, indexSettings);

        // Only enable block-level fetch if:
        // 1. Index is remote store enabled
        // 2. The required services are available
        // 3. We're not running on the primary shard
        boolean isBlockLevelFetchEnabled = isBlockLevelFetchEnabled(indexSettings);

        if (isBlockLevelFetchEnabled) {
            try {
                ShardId shardId = path.getShardId();
                // Check if this shard is a replica (not primary)
                if (shardRouting.primary() == false) {
                    logger.debug("Creating block fetch directory for replica shard {}", shardId);
                    assert transportService.isPresent() && clusterService.isPresent()
                        : "Transport service and cluster service must be present for block level fetch";

                    // Will be called by IndicesService when initializing the shard
                    return new PrimaryNodeBlockFetchDirectory(
                        baseDirectory,
                        shardId,
                        transportService.get().get(),
                        clusterService.get(),
                        () -> getPrimaryNodeId(shardId),
                        PRIMARY_BLOCK_SIZE_SETTING.get(indexSettings.getSettings())
                    );
                }
            } catch (Exception e) {
                // If there's any issue setting up block fetch, log and fall back to regular directory
                logger.warn("Failed to set up block level fetch directory, falling back to standard directory", e);
            }
        }
        return baseDirectory;
    }

    protected Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
        final String storeType = indexSettings.getSettings()
            .get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey());
        IndexModule.Type type;
        if (IndexModule.Type.FS.match(storeType)) {
            type = IndexModule.defaultStoreType(IndexModule.NODE_STORE_ALLOW_MMAP.get(indexSettings.getNodeSettings()));
        } else {
            type = IndexModule.Type.fromSettingsKey(storeType);
        }
        Set<String> preLoadExtensions = new HashSet<>(indexSettings.getValue(IndexModule.INDEX_STORE_PRE_LOAD_SETTING));
        switch (type) {
            case HYBRIDFS:
                // Use Lucene defaults
                final FSDirectory primaryDirectory = FSDirectory.open(location, lockFactory);
                final Set<String> nioExtensions = new HashSet<>(indexSettings.getValue(IndexModule.INDEX_STORE_HYBRID_NIO_EXTENSIONS));
                if (primaryDirectory instanceof MMapDirectory) {
                    MMapDirectory mMapDirectory = (MMapDirectory) primaryDirectory;
                    return new HybridDirectory(lockFactory, setPreload(mMapDirectory, preLoadExtensions), nioExtensions);
                } else {
                    return primaryDirectory;
                }
            case MMAPFS:
                return setPreload(new MMapDirectory(location, lockFactory), preLoadExtensions);
            // simplefs was removed in Lucene 9; support for enum is maintained for bwc
            case SIMPLEFS:
            case NIOFS:
                return new NIOFSDirectory(location, lockFactory);
            default:
                throw new AssertionError("unexpected built-in store type [" + type + "]");
        }
    }

    /**
     * Check if block level fetch is enabled for this index
     */
    private boolean isBlockLevelFetchEnabled(IndexSettings indexSettings) {
        boolean remoteStoreIndex = indexSettings.isAssignedOnRemoteNode();
        if (remoteStoreIndex == false) {
            logger.debug("{} is not assigned on a remote node", indexSettings.getIndex());
            return false;
        }
        boolean settingEnabled = PRIMARY_BLOCK_FETCH.get(indexSettings.getSettings());
        if (settingEnabled == false) {
            logger.debug("Block level fetch is not enabled for index {}", indexSettings.getIndex());
            return false;
        }
        boolean servicesAvailable = clusterService.isPresent() && transportService.isPresent();
        if (servicesAvailable == false) {
            logger.debug("Required services are not available for index {}", indexSettings.getIndex());
            return false;
        }

        return true;
    }

    /**
     * Get the primary node ID for a given shard
     */
    private String getPrimaryNodeId(ShardId shardId) {
        assert clusterService.isPresent();
        ClusterState state = clusterService.get().state();
        return state.routingTable().shardRoutingTable(shardId).primaryShard().currentNodeId();
    }

    public static MMapDirectory setPreload(MMapDirectory mMapDirectory, Set<String> preLoadExtensions) throws IOException {
        if (preLoadExtensions.isEmpty() == false) {
            mMapDirectory.setPreload(createPreloadPredicate(preLoadExtensions));
        }
        return mMapDirectory;
    }

    /**
     * Returns true iff the directory is a hybrid fs directory
     */
    public static boolean isHybridFs(Directory directory) {
        Directory unwrap = FilterDirectory.unwrap(directory);
        return unwrap instanceof HybridDirectory;
    }

    static BiPredicate<String, IOContext> createPreloadPredicate(Set<String> preLoadExtensions) {
        if (preLoadExtensions.contains("*")) {
            return MMapDirectory.ALL_FILES;
        } else {
            return (s, f) -> {
                int dotIndex = s.lastIndexOf('.');
                if (dotIndex > 0) {
                    return preLoadExtensions.contains(s.substring(dotIndex + 1));
                }
                return false;
            };
        }
    }

    /**
     * A hybrid directory implementation
     *
     * @opensearch.internal
     */
    static final class HybridDirectory extends NIOFSDirectory {
        private final MMapDirectory delegate;
        private final Set<String> nioExtensions;

        HybridDirectory(LockFactory lockFactory, MMapDirectory delegate, Set<String> nioExtensions) throws IOException {
            super(delegate.getDirectory(), lockFactory);
            this.delegate = delegate;
            this.nioExtensions = nioExtensions;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (useDelegate(name)) {
                // we need to do these checks on the outer directory since the inner doesn't know about pending deletes
                ensureOpen();
                ensureCanRead(name);
                // we only use the mmap to open inputs. Everything else is managed by the NIOFSDirectory otherwise
                // we might run into trouble with files that are pendingDelete in one directory but still
                // listed in listAll() from the other. We on the other hand don't want to list files from both dirs
                // and intersect for perf reasons.
                return delegate.openInput(name, context);
            } else {
                return super.openInput(name, context);
            }
        }

        boolean useDelegate(String name) {
            final String extension = FileSwitchDirectory.getExtension(name);
            return nioExtensions.contains(extension) == false;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(super::close, delegate);
        }

        MMapDirectory getDelegate() {
            return delegate;
        }
    }
}
