package hadoop.fs.cache;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;

import hadoop.fs.base.ContextFileSystem;

public class CacheFileSystem extends ContextFileSystem {

  private static final Map<String, LoadingCache<CacheKey, CacheData>> CACHES = new ConcurrentHashMap<>();

  private URI _cacheFsUri;
  private URI _realFsUri;
  private LoadingCache<CacheKey, CacheData> _cache;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    _cacheFsUri = name;
    String pathStr = conf.get(getConfigPrefix() + ".fs");
    Path path = new Path(pathStr);
    FileSystem fileSystem = path.getFileSystem(conf);
    _realFsUri = fileSystem.getUri();

    _cache = getCache(getConfigPrefix(), conf);
  }

  public static void cacheInvalidateAll() {
    Collection<LoadingCache<CacheKey, CacheData>> values = CACHES.values();
    CACHES.clear();
    for (LoadingCache<CacheKey, CacheData> cache : values) {
      cache.invalidateAll();
    }
  }

  public void cacheInvalidate() {
    _cache.invalidateAll();
  }

  private synchronized LoadingCache<CacheKey, CacheData> getCache(String fsPrefix, Configuration conf) {
    LoadingCache<CacheKey, CacheData> cache = CACHES.get(fsPrefix);
    if (cache != null) {
      return cache;
    }
    long maximumWeight = conf.getLong(getConfigPrefix() + ".max.size", 1_000_000_000L);
    Weigher<CacheKey, CacheData> weigher = (key, value) -> value.getSize();
    CacheLoader<CacheKey, CacheData> loader = key -> {
      File file = key.getFile();
      if (file.exists()) {
        return new CacheData(key, file);
      }
      return null;
    };
    RemovalListener<CacheKey, CacheData> removalListener = (key, value, cause) -> {
      if (value != null) {
        value.delete();
      }
    };
    CACHES.put(fsPrefix, cache = Caffeine.newBuilder()
                                         .maximumWeight(maximumWeight)
                                         .weigher(weigher)
                                         .removalListener(removalListener)
                                         .build(loader));
    return cache;
  }

  @Override
  public URI getUri() {
    return _cacheFsUri;
  }

  @Override
  public String getScheme() {
    return "cache";
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    f = makeQualified(f);
    FSDataInputStream inputStream = super.open(f, bufferSize);
    return new CacheFSDataInputStream(f, inputStream);
  }

  class CacheFSDataInputStream extends FSDataInputStream {

    private final Path _path;
    private final FSDataInputStream _in;

    public CacheFSDataInputStream(Path path, FSDataInputStream in) {
      super(in);
      _in = in;
      _path = path;
    }

  }

  @Override
  protected Path getOriginalPath(Path contextPath) throws IOException {
    return new Path(_cacheFsUri.getScheme(), _cacheFsUri.getAuthority(), contextPath.toUri()
                                                                                    .getPath());
  }

  @Override
  protected Path getContextPath(Path originalPath) throws IOException {
    return new Path(_realFsUri.getScheme(), _realFsUri.getAuthority(), originalPath.toUri()
                                                                                   .getPath());
  }

}
