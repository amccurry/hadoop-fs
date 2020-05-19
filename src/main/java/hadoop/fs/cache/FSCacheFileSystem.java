package hadoop.fs.cache;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import hadoop.fs.base.ContextFileSystem;

public class FSCacheFileSystem extends ContextFileSystem {

  private FSCache _fsCache;
  private URI _cacheFsUri;
  private URI _realFsUri;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    _fsCache = FSCache.getInstance(conf);
    _cacheFsUri = name;
    String pathStr = conf.get(getConfigPrefix() + ".fs");
    Path path = new Path(pathStr);
    FileSystem fileSystem = path.getFileSystem(conf);
    _realFsUri = fileSystem.getUri();
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
    Path contextPath = getContextPath(f);
    FileSystem contextFileSystem = contextPath.getFileSystem(getConf());
    FSDataInputStream inputStream = contextFileSystem.open(f, bufferSize);
    return new FSDataInputStream(
        new FSCachedInputStream(_fsCache, contextFileSystem.getFileStatus(contextPath), inputStream));
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
