package ru.shabarov.twolevelcache.cache;

import com.google.common.cache.Cache;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TwoLevelCacheTest {

    private Cache<String, String> cache;
    private final String PERSISTENCE_DIRACTORY_NAME = "tempDir";
    private final File PERSISTENCE_DIRECTORY_PATH = new File(PERSISTENCE_DIRACTORY_NAME);

    @After
    public void tearDown() throws Exception {
        cache.invalidateAll();
    }

    @Test
    public void testMaximumSizeStrategyNoOverflow() throws Exception {

        final long cacheSize = 10L;

        cache = TwoLevelCacheBuilder.newBuilder()
                .memoryMaximumSize(cacheSize)
                .persistanceMaximumSize(cacheSize)
                .persistenceDirectory(PERSISTENCE_DIRECTORY_PATH)
                .build();

        final long testSize = cacheSize * 2 - 1;
        List<KeyValuePair> keyValuePairs = KeyValuePair.makeTestElements(testSize);

        for (KeyValuePair keyValuePair : keyValuePairs) {
            cache.put(keyValuePair.getKey(), keyValuePair.getValue());
        }

        for (KeyValuePair keyValuePair : keyValuePairs) {
            String valueFromCache = cache.getIfPresent(keyValuePair.getKey());
            assertNotNull(valueFromCache);
            assertEquals(keyValuePair.getValue(), valueFromCache);
            assertEquals(testSize, cache.size());
        }
    }

    @Test
    public void testMaximumSizeStrategyWithOverflow() throws Exception {

        final long cacheSize = 10L;
        final long overflowObjectNum = 3;

        cache = TwoLevelCacheBuilder.newBuilder()
                .memoryMaximumSize(cacheSize)
                .persistanceMaximumSize(cacheSize)
                .persistenceDirectory(PERSISTENCE_DIRECTORY_PATH)
                .build();

        final long testSize = cacheSize * 2 + overflowObjectNum;
        List<KeyValuePair> keyValuePairs = KeyValuePair.makeTestElements(testSize);

        for (KeyValuePair keyValuePair : keyValuePairs) {
            cache.put(keyValuePair.getKey(), keyValuePair.getValue());
        }

        assertEquals(cacheSize * 2, cache.size());

        for (int i = 0; i < overflowObjectNum; i++) {
            String valueFromCache = cache.getIfPresent(KeyValuePair.makeKey(i));
            assertNull(valueFromCache);
        }
    }

    @Test
    public void testManualInvalidate() throws Exception {

        final long cacheSize = 10L;

        cache = TwoLevelCacheBuilder.newBuilder()
                .memoryMaximumSize(cacheSize)
                .persistanceMaximumSize(cacheSize)
                .persistenceDirectory(PERSISTENCE_DIRECTORY_PATH)
                .build();

        final long testSize = cacheSize * 2;
        List<KeyValuePair> keyValuePairs = KeyValuePair.makeTestElements(testSize);

        for (KeyValuePair keyValuePair : keyValuePairs) {
            cache.put(keyValuePair.getKey(), keyValuePair.getValue());
        }

        final int manualDeleteSize = 4, factor = 4;
        assertTrue(manualDeleteSize * factor < testSize);

        for (int i = 0; i < manualDeleteSize; i++) {
            int index = i * factor;
            cache.invalidate(KeyValuePair.makeKey(index));
            String value = cache.getIfPresent(KeyValuePair.makeKey(index));
            assertNull(value);
            assertEquals(cache.size(), testSize - i - 1);
        }

        cache.invalidateAll();
        assertEquals(cache.size(), 0);
    }

    @Test
    public void testTimeExpirationStrategy() throws Exception {

        final long cacheSize = 5L;
        final long expirationTime = 500;
        final int lookups = 10;
        final Random random = new Random();

        cache = TwoLevelCacheBuilder.newBuilder()
                .memoryMaximumSize(cacheSize)
                .expireAfterWrite(expirationTime, TimeUnit.MILLISECONDS)
                .expireAfterAccess(expirationTime, TimeUnit.MILLISECONDS)
                .persistenceDirectory(PERSISTENCE_DIRECTORY_PATH)
                .build();

        final long testSize = cacheSize * 2 - 1;
        List<KeyValuePair> keyValuePairs = KeyValuePair.makeTestElements(testSize);

        for (KeyValuePair keyValuePair : keyValuePairs) {
            cache.put(keyValuePair.getKey(), keyValuePair.getValue());
        }

        assertEquals(testSize, cache.size());

        //Multiple lookups to force eviction
        for (int i = 0; i < lookups; i++) {
            int randomKeyIndex = random.nextInt((int) testSize) + 1;
            cache.getIfPresent(KeyValuePair.makeKey(randomKeyIndex));
            Thread.sleep(expirationTime);
        }
        assertEquals(0, cache.size());
    }

    @Test
    public void testAddByCallable() throws Exception {

        final String callableReturnValue = "individual";

        cache = TwoLevelCacheBuilder.newBuilder()
                .persistenceDirectory(PERSISTENCE_DIRECTORY_PATH)
                .build();

        final Callable<String> callable = () -> callableReturnValue;

        String value0 = cache.get(KeyValuePair.makeKey(0), callable);
        assertNotNull(value0);
        assertEquals(value0, callableReturnValue);

        cache.put(KeyValuePair.makeKey(1), KeyValuePair.makeValue(1));
        String value1 = cache.getIfPresent(KeyValuePair.makeKey(1));
        assertNotNull(value1);
        assertEquals(value1, KeyValuePair.makeValue(1));

        value1 = cache.get(KeyValuePair.makeKey(1), callable);
        assertNotNull(value1);
        assertEquals(value1, KeyValuePair.makeValue(1));

        value0 = cache.getIfPresent(KeyValuePair.makeKey(0));
        assertNotNull(value0);
        assertEquals(value0, callableReturnValue);
    }
}
