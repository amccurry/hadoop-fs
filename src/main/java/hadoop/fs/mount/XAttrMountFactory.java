package hadoop.fs.mount;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hadoop.fs.util.FsUtil;

public class XAttrMountFactory extends Configured implements MountFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(XAttrMountFactory.class);

  private static final String TRUSTED_DIR_LINK = "trusted.dir.link";

  private final Mount _defaultMount;
  private UserGroupInformation _superUserUgi;

  public XAttrMountFactory(Mount defaultMount) {
    _defaultMount = defaultMount;
  }

  @Override
  public void initialize() {
    // need to make generic with kerberos support
    _superUserUgi = UserGroupInformation.createRemoteUser("hdfs");
  }

  @Override
  public Mount findMount(MountKey mountKey) throws IOException {
    LOGGER.info("findMount {}", mountKey);
    if (mountKey.getPathParts()
                .size() <= 1) {
      return null;
    }
    Path path = toPath(mountKey);
    LOGGER.info("Check mount from mount path {}", path);
    Path mountPath = _defaultMount.toMountPath(path);
    LOGGER.info("Mount path {}", mountPath);
    FileSystem fileSystem = mountPath.getFileSystem(getConf());
    try {
      return getMountFromPath(path, mountPath, fileSystem);
    } catch (FileNotFoundException | UnsupportedOperationException e) {
      LOGGER.error("Unknown error {}", e.getClass());
      return null;
    } catch (Exception e) {
      LOGGER.error("Unknown error {}", e.getClass());
      return null;
    }
  }

  private Mount getMountFromPath(Path path, Path mountPath, FileSystem fileSystem)
      throws IOException, InterruptedException {
    return _superUserUgi.doAs((PrivilegedExceptionAction<Mount>) () -> {
      Map<String, byte[]> xAttrs = fileSystem.getXAttrs(mountPath);
      byte[] bs = xAttrs.get(TRUSTED_DIR_LINK);
      if (bs == null) {
        LOGGER.info("No " + TRUSTED_DIR_LINK + " attribute found for file {}", mountPath);
        return null;
      }
      return new MountPathRewrite(new Path(new String(bs)), path);
    });

  }

  private Path toPath(MountKey mountKey) {
    return new Path(mountKey.getScheme(), mountKey.getAuthority(), FsUtil.PATH_JOINER.join(mountKey.getPathParts()));
  }
}
