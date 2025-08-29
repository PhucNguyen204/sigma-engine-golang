package matcher

import (
	"regexp"
	"sync"
	"time"
)

// GlobalRegexCache provides thread-safe caching of compiled regex patterns
type GlobalRegexCache struct {
	cache   map[string]*CachedRegex
	mutex   sync.RWMutex
	config  CacheConfig
	stats   CacheStats
}

// CachedRegex represents a cached compiled regex with metadata
type CachedRegex struct {
	Regex       *regexp.Regexp
	Pattern     string
	CompiledAt  time.Time
	AccessCount int64
	LastAccess  time.Time
	IsHot       bool // Frequently accessed patterns
}

// CacheConfig contains configuration for the regex cache
type CacheConfig struct {
	MaxSize         int           // Maximum number of cached patterns
	TTL             time.Duration // Time to live for cached patterns
	HotThreshold    int64         // Access count threshold for hot patterns
	CleanupInterval time.Duration // Interval for cleanup of expired patterns
}

// CacheStats contains statistics about cache performance
type CacheStats struct {
	Hits            int64
	Misses          int64
	Compilations    int64
	Evictions       int64
	CurrentSize     int
	HotPatterns     int
	MemoryUsage     int64
}

// Global instance of regex cache
var globalCache *GlobalRegexCache
var cacheOnce sync.Once

// GetGlobalCache returns the singleton global regex cache
func GetGlobalCache() *GlobalRegexCache {
	cacheOnce.Do(func() {
		globalCache = NewGlobalRegexCache(DefaultCacheConfig())
		globalCache.startCleanupRoutine()
	})
	return globalCache
}

// DefaultCacheConfig returns default cache configuration
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		MaxSize:         1000,
		TTL:             30 * time.Minute,
		HotThreshold:    100,
		CleanupInterval: 5 * time.Minute,
	}
}

// NewGlobalRegexCache creates a new regex cache with the given configuration
func NewGlobalRegexCache(config CacheConfig) *GlobalRegexCache {
	return &GlobalRegexCache{
		cache:  make(map[string]*CachedRegex),
		config: config,
		stats:  CacheStats{},
	}
}

// GetOrCompile retrieves a compiled regex from cache or compiles and caches it
func (c *GlobalRegexCache) GetOrCompile(pattern string) (*regexp.Regexp, error) {
	c.mutex.RLock()
	cached, exists := c.cache[pattern]
	if exists {
		// Update access statistics
		cached.AccessCount++
		cached.LastAccess = time.Now()
		if cached.AccessCount >= c.config.HotThreshold {
			cached.IsHot = true
		}
		c.stats.Hits++
		c.mutex.RUnlock()
		return cached.Regex, nil
	}
	c.mutex.RUnlock()

	// Cache miss - need to compile
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Double-check locking pattern
	if cached, exists := c.cache[pattern]; exists {
		cached.AccessCount++
		cached.LastAccess = time.Now()
		c.stats.Hits++
		return cached.Regex, nil
	}

	// Compile the regex
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	// Check if cache is full and evict if necessary
	if len(c.cache) >= c.config.MaxSize {
		c.evictOldest()
	}

	// Add to cache
	now := time.Now()
	c.cache[pattern] = &CachedRegex{
		Regex:       compiled,
		Pattern:     pattern,
		CompiledAt:  now,
		AccessCount: 1,
		LastAccess:  now,
		IsHot:       false,
	}

	c.stats.Misses++
	c.stats.Compilations++
	c.stats.CurrentSize = len(c.cache)

	return compiled, nil
}

// evictOldest removes the oldest non-hot pattern from cache
func (c *GlobalRegexCache) evictOldest() {
	var oldestKey string
	var oldestTime time.Time
	var found bool

	// First pass: try to evict non-hot patterns
	for key, cached := range c.cache {
		if !cached.IsHot {
			if !found || cached.LastAccess.Before(oldestTime) {
				oldestKey = key
				oldestTime = cached.LastAccess
				found = true
			}
		}
	}

	// If no non-hot patterns found, evict the oldest hot pattern
	if !found {
		for key, cached := range c.cache {
			if !found || cached.LastAccess.Before(oldestTime) {
				oldestKey = key
				oldestTime = cached.LastAccess
				found = true
			}
		}
	}

	if found {
		delete(c.cache, oldestKey)
		c.stats.Evictions++
	}
}

// startCleanupRoutine starts a background goroutine to clean up expired patterns
func (c *GlobalRegexCache) startCleanupRoutine() {
	go func() {
		ticker := time.NewTicker(c.config.CleanupInterval)
		defer ticker.Stop()

		for range ticker.C {
			c.cleanup()
		}
	}()
}

// cleanup removes expired patterns from cache
func (c *GlobalRegexCache) cleanup() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now()
	var toDelete []string

	for key, cached := range c.cache {
		// Don't evict hot patterns during cleanup
		if cached.IsHot {
			continue
		}

		// Check if pattern has expired
		if now.Sub(cached.LastAccess) > c.config.TTL {
			toDelete = append(toDelete, key)
		}
	}

	// Remove expired patterns
	for _, key := range toDelete {
		delete(c.cache, key)
		c.stats.Evictions++
	}

	c.stats.CurrentSize = len(c.cache)
	
	// Update hot patterns count
	hotCount := 0
	for _, cached := range c.cache {
		if cached.IsHot {
			hotCount++
		}
	}
	c.stats.HotPatterns = hotCount
}

// GetStats returns current cache statistics
func (c *GlobalRegexCache) GetStats() CacheStats {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	
	stats := c.stats
	stats.CurrentSize = len(c.cache)
	
	// Calculate memory usage estimate
	memoryUsage := int64(0)
	for _, cached := range c.cache {
		// Rough estimate: pattern string + regex structure
		memoryUsage += int64(len(cached.Pattern)) + 1000 // 1KB estimate per compiled regex
	}
	stats.MemoryUsage = memoryUsage
	
	return stats
}

// Clear removes all patterns from cache
func (c *GlobalRegexCache) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	c.cache = make(map[string]*CachedRegex)
	c.stats = CacheStats{}
}

// GetHitRatio returns cache hit ratio as a percentage
func (c *GlobalRegexCache) GetHitRatio() float64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	
	total := c.stats.Hits + c.stats.Misses
	if total == 0 {
		return 0.0
	}
	
	return float64(c.stats.Hits) / float64(total) * 100.0
}

// CreateCachedRegexMatch creates a regex matcher that uses the global cache
func CreateCachedRegexMatch() MatchFn {
	cache := GetGlobalCache()
	
	return func(fieldValue string, patterns []string, modifiers []string) (bool, error) {
		for _, pattern := range patterns {
			regex, err := cache.GetOrCompile(pattern)
			if err != nil {
				continue // Skip invalid patterns
			}
			
			if regex.MatchString(fieldValue) {
				return true, nil
			}
		}
		
		return false, nil
	}
}
