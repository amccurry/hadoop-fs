package hadoop.fs.mount;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hadoop.fs.base.ContextFileSystem;
import hadoop.fs.base.PathContext;
import hadoop.fs.util.TimerCloseable;
import hadoop.fs.util.TimerUtil;

public class MountFileSystem extends ContextFileSystem {

  private static final Logger LOGGER = LoggerFactory.getLogger(MountFileSystem.class);

  private URI _uri;
  private MountManager _mountManager;

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
    _mountManager.addMount(realPath, makeQualified(virtualPath), persistant);
  }

  public void reloadMounts() throws IOException {
    _mountManager.reloadMounts();
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    _uri = uri;
    _mountManager = MountManager.getInstance(conf, getConfigPrefix(), getRootPath());
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
    try (TimerCloseable time = TimerUtil.time(LOGGER, "getPathContext", f.toString())) {
      Path originalPath = makeQualified(f);
      Mount mount = getMount(originalPath);
      LOGGER.info("path {} mount {}", f, mount);
      Path mountPath = mount.toMountPath(originalPath);
      LOGGER.info("original path {} to mount path {}", originalPath, mountPath);
      return new PathContext() {

        @Override
        public Path getOriginalPath() {
          try (TimerCloseable t = TimerUtil.time(LOGGER, "getOriginalPath")) {
            return originalPath;
          }
        }

        @Override
        public Path getOriginalPath(Path contextPath) throws IOException {
          try (TimerCloseable t = TimerUtil.time(LOGGER, "getOriginalPath", contextPath.toString())) {
            return mount.fromMountPath(contextPath);
          }
        }

        @Override
        public Path getContextPath() {
          return mountPath;
        }
      };
    }
  }

  @Override
  protected Path getOriginalPath(Path realPath) throws IOException {
    throw new IOException("Not Implemented");
  }

  @Override
  protected Path getContextPath(Path chrootPath) throws IOException {
    throw new IOException("Not Implemented");
  }

  @Override
  public void setXAttr(Path p, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
    if (name.equals("trusted.mount")) {
      _mountManager.addMount(new Path(new String(value)), p, true);
    } else {
      super.setXAttr(p, name, value, flag);
    }
  }

  private Mount getMount(Path path) throws IOException {
    return _mountManager.getMount(path);
  }

  private Path getRootPath() {
    return new Path(_uri.getScheme(), _uri.getAuthority(), "/");
  }

}
