package hadoop.fs.mount;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hadoop.fs.util.FsUtil;

public class XAttrMountFactory extends Configured implements MountFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(XAttrMountFactory.class);

  public static final String XATTR_HADOOP_AUTHENTICATION_KERBEROS_PRINCIPAL = "xattr.hadoop.authentication.kerberos.principal";
  public static final String XATTR_HADOOP_AUTHENTICATION_KERBEROS_KEYTAB = "xattr.hadoop.authentication.kerberos.keytab";
  public static final String HDFS = "hdfs";
  public static final String TRUSTED_DIR_LINK = "trusted.dir.link";

  private final Mount _defaultMount;

  private UserGroupInformation _superUserUgi;

  public XAttrMountFactory(Mount defaultMount) throws IOException {
    _defaultMount = defaultMount;
  }

  @Override
  public void initialize() throws IOException {
    _superUserUgi = getUserGroupInformation(getConf());
  }

  public static UserGroupInformation getUserGroupInformation(Configuration configuration) throws IOException {
    String principal = configuration.get(XATTR_HADOOP_AUTHENTICATION_KERBEROS_PRINCIPAL);
    if (principal == null) {
      return UserGroupInformation.createRemoteUser(HDFS);
    } else {
      String keytab = configuration.get(XATTR_HADOOP_AUTHENTICATION_KERBEROS_KEYTAB);
      return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
    }
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
    try {
      return getMountFromPath(_superUserUgi, path, mountPath, getConf());
    } catch (FileNotFoundException | UnsupportedOperationException e) {
      LOGGER.error("Unknown error {}", e.getClass());
      return null;
    } catch (Exception e) {
      LOGGER.error("Unknown error {}", e.getClass());
      return null;
    }
  }

  public static Mount getMountFromPath(UserGroupInformation ugi, Path path, Path mountPath, Configuration configuration)
      throws IOException, InterruptedException {
    ugi.checkTGTAndReloginFromKeytab();
    return ugi.doAs((PrivilegedExceptionAction<Mount>) () -> {
      FileSystem fileSystem = mountPath.getFileSystem(configuration);
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
