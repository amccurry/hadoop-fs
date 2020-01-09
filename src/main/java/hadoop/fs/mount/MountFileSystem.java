package hadoop.fs.mount;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import hadoop.fs.base.ContextFileSystem;
import hadoop.fs.base.PathContext;

public class MountFileSystem extends ContextFileSystem {

  private URI _uri;
  private MountManager _mountCache;

  public static void addMount(Configuration conf, Path realPath, Path virtualPath) throws IOException {
    addMount(conf, realPath, virtualPath, false);
  }

  public static void addMount(Configuration conf, Path realPath, Path virtualPath, boolean persistant)
      throws IOException {
    MountFileSystem fileSystem = (MountFileSystem) virtualPath.getFileSystem(conf);
    fileSystem.addMount(realPath, virtualPath, persistant);
  }

  public Path getRealPath(Path virtualPath) throws IOException {
    Path path = makeQualified(virtualPath);
    PathContext pathContext = getPathContext(path);
    return pathContext.getContextPath();
  }

  public void addMount(Path realPath, Path virtualPath) throws IOException {
    addMount(realPath, virtualPath, false);
  }

  public void addMount(Path realPath, Path virtualPath, boolean persistant) throws IOException {
    FileSystem fileSystem = realPath.getFileSystem(getConf());
    realPath = fileSystem.makeQualified(realPath);
    _mountCache.addMount(realPath, makeQualified(virtualPath), persistant);
  }

  public void reloadMounts() throws IOException {
    _mountCache.reloadMounts();
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    _uri = uri;
    _mountCache = MountManager.getInstance(conf, getConfigPrefix(), getRootPath());
  }

  @Override
  public URI getUri() {
    return _uri;
  }

  @Override
  public String getScheme() {
    return "mount";
  }

  @Override
  protected PathContext getPathContext(Path f) throws IOException {
    Path originalPath = makeQualified(f);
    Mount mount = getMount(f);
    Path mountPath = mount.toMountPath(originalPath);
    return new PathContext() {

      @Override
      public Path getOriginalPath() {
        return originalPath;
      }

      @Override
      public Path getOriginalPath(Path contextPath) throws IOException {
        return mount.fromMountPath(contextPath);
      }

      @Override
      public Path getContextPath() {
        return mountPath;
      }
    };
  }

  @Override
  protected Path getOriginalPath(Path realPath) throws IOException {
    throw new IOException("Not Implemented");
  }

  @Override
  protected Path getContextPath(Path chrootPath) throws IOException {
    throw new IOException("Not Implemented");
  }

  private Mount getMount(Path path) {
    return _mountCache.getMount(makeQualified(path));
  }

  private Path getRootPath() {
    return new Path(_uri.getScheme(), _uri.getAuthority(), "/");
  }

}
