package ru.shabarov.twolevelcache.cache;

import com.google.common.base.Ticker;
import com.google.common.cache.*;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class TwoLevelCacheBuilder<K, V> {

    private final CacheBuilder<Object, Object> underlyingCacheBuilder;

    private Long maximumSize;
    private Long evictionAfterWriteTime;
    private Long evictionAfterAccessTime;

    private RemovalListener<? super K, ? super V> removalListener;
    private File persistenceDirectory;

    public static TwoLevelCacheBuilder<Object, Object> newBuilder() {
        return new TwoLevelCacheBuilder<>();
    }

    private TwoLevelCacheBuilder() {
        this.underlyingCacheBuilder = CacheBuilder.newBuilder();
    }

    public TwoLevelCacheBuilder<K, V> concurrencyLevel(int concurrencyLevel) {
        underlyingCacheBuilder.concurrencyLevel(concurrencyLevel);
        return this;
    }

    public TwoLevelCacheBuilder<K, V> expireAfterAccess(long duration, TimeUnit unit) {
        underlyingCacheBuilder.expireAfterAccess(duration, unit);
        this.evictionAfterAccessTime = unit.toMillis(duration);
        return this;
    }

    public TwoLevelCacheBuilder<K, V> expireAfterWrite(long duration, TimeUnit unit) {
        underlyingCacheBuilder.expireAfterWrite(duration, unit);
        this.evictionAfterWriteTime = unit.toMillis(duration);
        return this;
    }

    public TwoLevelCacheBuilder<K, V> refreshAfterWrite(long duration, TimeUnit unit) {
        underlyingCacheBuilder.refreshAfterWrite(duration, unit);
        this.evictionAfterWriteTime = unit.toMillis(duration);
        return this;
    }

    public TwoLevelCacheBuilder<K, V> initialCapacity(int initialCapacity) {
        underlyingCacheBuilder.initialCapacity(initialCapacity);
        return this;
    }

    public TwoLevelCacheBuilder<K, V> memoryMaximumSize(long size) {
        underlyingCacheBuilder.maximumSize(size);
        return this;
    }

    public TwoLevelCacheBuilder<K, V> persistanceMaximumSize(long size) {
        this.maximumSize = size;
        return this;
    }

    public TwoLevelCacheBuilder<K, V> maximumWeight(long weight) {
        underlyingCacheBuilder.maximumWeight(weight);
        return this;
    }

    public TwoLevelCacheBuilder<K, V> recordStats() {
        underlyingCacheBuilder.recordStats();
        return this;
    }

    public TwoLevelCacheBuilder<K, V> softValues() {
        underlyingCacheBuilder.softValues();
        return this;
    }

    public TwoLevelCacheBuilder<K, V> weakKeys() {
        underlyingCacheBuilder.weakKeys();
        return this;
    }

    public TwoLevelCacheBuilder<K, V> weakValues() {
        underlyingCacheBuilder.weakValues();
        return this;
    }

    public TwoLevelCacheBuilder<K, V> ticker(Ticker ticker) {
        underlyingCacheBuilder.ticker(ticker);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <K1 extends K, V1 extends V> TwoLevelCacheBuilder<K1, V1> weigher(Weigher<? super K1, ? super V1> weigher) {
        underlyingCacheBuilder.weigher(weigher);
        return (TwoLevelCacheBuilder<K1, V1>) this;
    }

    public <K1 extends K, V1 extends V> TwoLevelCacheBuilder<K1, V1> removalListener(RemovalListener<? super K1, ? super V1> listener) {
        checkState(this.removalListener == null);
        @SuppressWarnings("unchecked")
        TwoLevelCacheBuilder<K1, V1> castThis = (TwoLevelCacheBuilder<K1, V1>) this;
        castThis.removalListener = checkNotNull(listener);
        return castThis;
    }

    /**
     * Sets a location for persisting files. This directory <b>must not be used for other purposes</b>.
     *
     * @param persistenceDirectory A directory which is used by this file cache.
     * @return This builder.
     */
    public TwoLevelCacheBuilder<K, V> persistenceDirectory(File persistenceDirectory) {
        checkState(this.persistenceDirectory == null);
        this.persistenceDirectory = checkNotNull(persistenceDirectory);
        return this;
    }

    public <K1 extends K, V1 extends V> Cache<K1, V1> build() {
        if (persistenceDirectory == null) {
            return new TwoLevelCache<>(underlyingCacheBuilder,
                    TwoLevelCacheBuilder.<K1, V1>castRemovalListener(removalListener),
                    maximumSize, evictionAfterWriteTime, evictionAfterAccessTime);
        } else {
            return new TwoLevelCache<>(underlyingCacheBuilder, persistenceDirectory,
                    TwoLevelCacheBuilder.<K1, V1>castRemovalListener(removalListener),
                    maximumSize, evictionAfterWriteTime, evictionAfterAccessTime);
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> RemovalListener<K, V> castRemovalListener(RemovalListener<?, ?> removalListener) {
        if (removalListener == null) {
            return null;
        } else {
            return (RemovalListener<K, V>) removalListener;
        }
    }

    @Override
    public String toString() {
        return "TwoLevelCacheBuilder{" +
                "underlyingCacheBuilder=" + underlyingCacheBuilder +
                ", maximumSize=" + maximumSize +
                ", evictionAfterWriteTime=" + evictionAfterWriteTime +
                ", removalListener=" + removalListener +
                ", persistenceDirectory=" + persistenceDirectory +
                '}';
    }
}
