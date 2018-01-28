package ru.shabarov.twolevelcache.cache;

import com.blogspot.mydailyjava.guava.cache.overflow.FileSystemPersistingCache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shabarov.twolevelcache.exception.NotEvictedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TwoLevelCache<K, V> extends FileSystemPersistingCache<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwoLevelCache.class);

    private Optional<Long> maximumSize;
    private Optional<Long> evictionAfterAccessTime;
    private Optional<Long> evictionAfterWriteTime;
    private final ExecutorService timeExpirationExecutorService = Executors.newSingleThreadExecutor();

    protected TwoLevelCache(CacheBuilder<Object, Object> cacheBuilder, RemovalListener<K, V> removalListener,
                            Long maximumSize, Long evictionAfterAccessTime, Long evictionAfterWriteTime) {
        super(cacheBuilder, removalListener);
        checkCoherentAndAssign(maximumSize, evictionAfterAccessTime, evictionAfterWriteTime);
    }

    protected TwoLevelCache(CacheBuilder<Object, Object> cacheBuilder, File persistenceDirectory,
                            RemovalListener<K, V> removalListener, Long maximumSize, Long evictionAfterAccessTime,
                            Long evictionAfterWriteTime) {
        super(cacheBuilder, persistenceDirectory, removalListener);
        checkCoherentAndAssign(maximumSize, evictionAfterAccessTime, evictionAfterWriteTime);
    }

    @Override
    protected void persistValue(K key, V value) throws IOException {
        V foundedValue = findPersistedAndExpire(key);
        if (foundedValue == null || !foundedValue.equals(value)) {
            if (maximumSize.isPresent() && sizeOfPersisted() >= maximumSize.get()) {
                File persistenceRootDirectory = getPersistenceRootDirectory();
                Optional<Path> expiredCacheFile = getExpiredCacheFile(persistenceRootDirectory);
                if (expiredCacheFile.isPresent()) {
                    File file = expiredCacheFile.get().toFile();
                    boolean isDeleted = file.delete();
                    if (!isDeleted) {
                        throw new NotEvictedException("Couldn't evict cache file=" + file.getAbsolutePath() +
                                " when maximum size is reached");
                    }
                }
            }
            super.persistValue(key, value);
        } else {
            LOGGER.trace(String.format("Object with key=%s and value=%s is already persisted", key, value));
        }
    }

    @Override
    protected V findPersisted(K key) throws IOException {
        V foundedValue = super.findPersisted(key);
        if (evictionAfterAccessTime.isPresent()) {
            this.timeExpirationExecutorService.submit(new TimeExpiryWorker(evictionAfterAccessTime.get()));
        }
        return foundedValue;
    }

    protected V findPersistedAndExpire(K key) throws IOException {
        V foundedValue = super.findPersisted(key);
        if (evictionAfterWriteTime.isPresent()) {
            this.timeExpirationExecutorService.submit(new TimeExpiryWorker(evictionAfterWriteTime.get()));
        }
        return foundedValue;
    }

    private Optional<Path> getExpiredCacheFile(File persistenceRootDir) throws IOException {
        Path dir = Paths.get(persistenceRootDir.getAbsolutePath());
        return Files.list(dir)
                .filter(f -> !Files.isDirectory(f))
                .min(Comparator.comparingLong(f -> f.toFile().lastModified()));
    }

    private void checkCoherentAndAssign(Long maximumSize, Long evictionAfterAccessTime, Long evictionAfterWriteTime) {
        this.maximumSize = Optional.ofNullable(maximumSize);
        this.evictionAfterAccessTime = Optional.ofNullable(evictionAfterAccessTime);
        this.evictionAfterWriteTime = Optional.ofNullable(evictionAfterWriteTime);
        if ((this.maximumSize.isPresent() && this.evictionAfterAccessTime.isPresent()) ||
                (this.maximumSize.isPresent() && this.evictionAfterWriteTime.isPresent())) {
            throw new IllegalStateException("Simultaneous max size and time eviction strategy is not allowed");
        }
    }

    private class TimeExpiryWorker implements Runnable {

        private final long expirationTime;

        public TimeExpiryWorker(long expirationTime) {
            this.expirationTime = expirationTime;
        }

        @Override
        public void run() {
            try {
                LOGGER.trace("Time expiration worker has been started");
                Path rootDirectory = Paths.get(getPersistenceRootDirectory().getAbsolutePath());
                long actualTime = System.currentTimeMillis();
                Object[] expiredFiles = Files.list(rootDirectory)
                        .filter(f -> !Files.isDirectory(f))
                        .filter(f -> actualTime - f.toFile().lastModified() > expirationTime).toArray();
                if (ArrayUtils.isNotEmpty(expiredFiles)) {
                    for (Object object : expiredFiles) {
                        File file = ((Path) object).toFile();
                        boolean isDeleted = file.delete();
                        if (!isDeleted) {
                            LOGGER.trace("Couldn't evict cache file=" + file.getAbsolutePath() +
                                    " when time expiration is reached");
                        } else {
                            LOGGER.trace("Cache file=" + file.getAbsolutePath() +
                                    " has been evicted");
                        }
                    }
                } else {
                    LOGGER.trace("No time expired file caches found");
                }
                LOGGER.trace("Time expiration worker has been finished");
            } catch (Exception e) {
                LOGGER.error("Time expiration worker has interrupted by internal error", e);
            }
        }
    }
}
