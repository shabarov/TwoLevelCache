# twolevelcache
Implementation of a lightweigth cache with two levels - memory and files, based on a FileSystemPersistingCache https://github.com/raphw/guava-cache-overflow-extension

Cache should be created by builder:

Cache cache = TwoLevelCacheBuilder.newBuilder()
                .memoryMaximumSize(cacheSize)
                .persistanceMaximumSize(cacheSize)
                .expireAfterWrite(expirationTime, TimeUnit.MILLISECONDS)
                .expireAfterAccess(expirationTime, TimeUnit.MILLISECONDS)
                .persistenceDirectory(PERSISTENCE_DIRECTORY_PATH)
                .build();
                
Guava LoadingCache is used for a first level and FileSystemPersistingCache features for second.
Client can setup maximum size for a memory and file cache separately for size-based eviction strategy, and expiration time for time-based eviction strategy.  
