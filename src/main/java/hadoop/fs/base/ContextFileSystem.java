package hadoop.fs.base;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hadoop.fs.util.*;

public abstract class ContextFileSystem extends FileSystem {

  private static final Path ROOT = new Path("/");
  private static final String USER_HOME_DIR_PREFIX = "/user/";

  private final Logger LOGGER = LoggerFactory.getLogger(getClass());
  private Path _workingDir;

  protected PathContext getPathContext(Path originalPath) throws IOException {
    Path contextPath = getContextPath(originalPath);

    return new PathContext() {

      @Override
      public Path getOriginalPath(Path contextPath) throws IOException {
        return ContextFileSystem.this.getOriginalPath(contextPath);
      }

      @Override
      public Path getOriginalPath() throws IOException {
        return originalPath;
      }

      @Override
      public Path getContextPath() throws IOException {
        return contextPath;
      }
    };
  }

  protected abstract Path getOriginalPath(Path contextPath) throws IOException;

  protected abstract Path getContextPath(Path originalPath) throws IOException;

  @Override
  public abstract String getScheme();

  public String getConfigPrefix() {
    return getScheme() + "." + getUri().getAuthority();
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    try (TimerCloseable time = TimerUtil.time(LOGGER, "open", f.toString())) {
      LOGGER.info("open {} {}", f, bufferSize);
      PathContext context = getPathContext(f);
      Path path = context.getContextPath();
      try {
        FileSystem fileSystem = path.getFileSystem(getConf());
        return fileSystem.open(path, bufferSize);
      } catch (IOException e) {
        LOGGER.debug(e.getMessage(), e);
        throw handleError(e, context, path);
      }
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    try (TimerCloseable time = TimerUtil.time(LOGGER, "create", f.toString())) {
      PathContext context = getPathContext(f);
      Path path = context.getContextPath();
      try {
        FileSystem fileSystem = path.getFileSystem(getConf());
        return fileSystem.create(path, overwrite, bufferSize, replication, blockSize, progress);
      } catch (IOException e) {
        LOGGER.debug(e.getMessage(), e);
        throw handleError(e, context, path);
      }
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    try (TimerCloseable time = TimerUtil.time(LOGGER, "append", f.toString())) {
      PathContext context = getPathContext(f);
      Path path = context.getContextPath();
      try {
        FileSystem fileSystem = path.getFileSystem(getConf());
        return fileSystem.append(path, bufferSize, progress);
      } catch (IOException e) {
        LOGGER.debug(e.getMessage(), e);
        throw handleError(e, context, path);
      }
    }
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    try (TimerCloseable time = TimerUtil.time(LOGGER, "rename", src.toString(), dst.toString())) {
      PathContext srcContext = getPathContext(src);
      PathContext dstContext = getPathContext(dst);

      Path srcPath = srcContext.getContextPath();
      Path dstPath = dstContext.getContextPath();

      FileSystem srcFileSystem = srcPath.getFileSystem(getConf());
      FileSystem dstFileSystem = dstPath.getFileSystem(getConf());

      if (!isSameFileSystem(srcFileSystem, dstFileSystem)) {
        return false;
      }
      return srcFileSystem.rename(srcPath, dstPath);
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    try (TimerCloseable time = TimerUtil.time(LOGGER, "delete", f.toString())) {
      PathContext context = getPathContext(f);
      Path path = context.getContextPath();
      try {
        FileSystem fileSystem = path.getFileSystem(getConf());
        return fileSystem.delete(path, recursive);
      } catch (IOException e) {
        LOGGER.debug(e.getMessage(), e);
        throw handleError(e, context, path);
      }
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    try (TimerCloseable time = TimerUtil.time(LOGGER, "listStatus", f.toString())) {
      PathContext context = getPathContext(f);
      Path path = context.getContextPath();
      try {
        FileSystem fileSystem = path.getFileSystem(getConf());
        return getFileStatus(context, fileSystem.listStatus(path));
      } catch (IOException e) {
        LOGGER.debug(e.getMessage(), e);
        throw handleError(e, context, path);
      }
    }
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    try (TimerCloseable time = TimerUtil.time(LOGGER, "mkdirs", f.toString())) {
      PathContext context = getPathContext(f);
      Path path = context.getContextPath();
      try {
        FileSystem fileSystem = path.getFileSystem(getConf());
        return fileSystem.mkdirs(path, permission);
      } catch (IOException e) {
        LOGGER.debug(e.getMessage(), e);
        throw handleError(e, context, path);
      }
    }
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    try (TimerCloseable time = TimerUtil.time(LOGGER, "getFileStatus", f.toString())) {
      LOGGER.info("getFileStatus {}", f);
      PathContext context = getPathContext(f);
      Path path = context.getContextPath();
      try {
        FileSystem fileSystem = path.getFileSystem(getConf());
        return getFileStatus(context, fileSystem.getFileStatus(path));
      } catch (IOException e) {
        LOGGER.debug(e.getMessage(), e);
        throw handleError(e, context, path);
      }
    }
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    _workingDir = new_dir;
  }

  @Override
  public Path getWorkingDirectory() {
    return _workingDir;
  }

  @Override
  public Path getHomeDirectory() {
    try {
      return makeQualified(new Path(USER_HOME_DIR_PREFIX + UserGroupInformation.getCurrentUser()
                                                                               .getShortUserName()));
    } catch (IllegalArgumentException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      fileSystem.setPermission(path, permission);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      fileSystem.setOwner(path, username, groupname);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      fileSystem.setTimes(path, mtime, atime);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public void setAcl(Path p, List<AclEntry> aclSpec) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      fileSystem.setAcl(path, aclSpec);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public void setXAttr(Path p, String name, byte[] value) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      fileSystem.setXAttr(path, name, value);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public void setXAttr(Path p, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      fileSystem.setXAttr(path, name, value, flag);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public AclStatus getAclStatus(Path p) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      return fileSystem.getAclStatus(path);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    try (TimerCloseable time = TimerUtil.time(LOGGER, "getDelegationToken", renewer)) {
      PathContext context = getPathContext(ROOT);
      Path path = context.getContextPath();
      try {
        FileSystem fileSystem = path.getFileSystem(getConf());
        return fileSystem.getDelegationToken(renewer);
      } catch (IOException e) {
        LOGGER.debug(e.getMessage(), e);
        throw handleError(e, context, path);
      }
    }
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws IOException {
    try (TimerCloseable time = TimerUtil.time(LOGGER, "addDelegationTokens", renewer)) {
      PathContext context = getPathContext(ROOT);
      Path path = context.getContextPath();
      try {
        FileSystem fileSystem = path.getFileSystem(getConf());
        return fileSystem.addDelegationTokens(renewer, credentials);
      } catch (IOException e) {
        LOGGER.debug(e.getMessage(), e);
        throw handleError(e, context, path);
      }
    }
  }

  @Override
  public void modifyAclEntries(Path p, List<AclEntry> aclSpec) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      fileSystem.modifyAclEntries(path, aclSpec);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public byte[] getXAttr(Path p, String name) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      return fileSystem.getXAttr(path, name);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path p) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      return fileSystem.getXAttrs(path);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path p, List<String> names) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      return fileSystem.getXAttrs(path, names);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public List<String> listXAttrs(Path p) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      return fileSystem.listXAttrs(path);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public void removeAclEntries(Path p, List<AclEntry> aclSpec) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      fileSystem.removeAclEntries(path, aclSpec);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public void removeDefaultAcl(Path p) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      fileSystem.removeDefaultAcl(path);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public void removeAcl(Path p) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      fileSystem.removeAcl(path);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public void removeXAttr(Path p, String name) throws IOException {
    PathContext context = getPathContext(p);
    Path path = context.getContextPath();
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      fileSystem.removeXAttr(path, name);
    } catch (IOException e) {
      LOGGER.debug(e.getMessage(), e);
      throw handleError(e, context, path);
    }
  }

  @Override
  public FileStatus getFileLinkStatus(Path f)
      throws AccessControlException, FileNotFoundException, UnsupportedFileSystemException, IOException {
    return super.getFileLinkStatus(f);
  }

  @Override
  public Path makeQualified(Path path) {
    return super.makeQualified(path);
  }

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    return super.create(f);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
    return super.create(f, overwrite);
  }

  @Override
  public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
    return super.create(f, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, short replication) throws IOException {
    return super.create(f, replication);
  }

  @Override
  public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
    return super.create(f, replication, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
    return super.create(f, overwrite, bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress)
      throws IOException {
    return super.create(f, overwrite, bufferSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
      throws IOException {
    return super.create(f, overwrite, bufferSize, replication, blockSize);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return super.create(f, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    return super.create(f, permission, flags, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress, ChecksumOpt checksumOpt) throws IOException {
    return super.create(f, permission, flags, bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  public boolean createNewFile(Path f) throws IOException {
    return super.createNewFile(f);
  }

  @Override
  public void copyFromLocalFile(Path src, Path dst) throws IOException {
    super.copyFromLocalFile(src, dst);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    super.copyFromLocalFile(delSrc, src, dst);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
    super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
    super.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  @Override
  public void copyToLocalFile(Path src, Path dst) throws IOException {
    super.copyToLocalFile(src, dst);
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    super.copyToLocalFile(delSrc, src, dst);
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem) throws IOException {
    super.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
  }

  @Override
  public FSDataOutputStream append(Path f) throws IOException {
    return super.append(f);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    return super.append(f, bufferSize);
  }

  @Override
  public boolean deleteOnExit(Path f) throws IOException {
    return super.deleteOnExit(f);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    return super.exists(f);
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    super.completeLocalOutput(fsOutputFile, tmpLocalFile);
  }

  @Override
  public void access(Path path, FsAction mode) throws AccessControlException, FileNotFoundException, IOException {
    super.access(path, mode);
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    return super.open(f);
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    return super.isDirectory(f);
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    return super.isFile(f);
  }

  @Override
  public FileStatus[] listStatus(Path f, PathFilter filter) throws FileNotFoundException, IOException {
    return super.listStatus(f, filter);
  }

  @Override
  public FileStatus[] listStatus(Path[] files) throws FileNotFoundException, IOException {
    return super.listStatus(files);
  }

  @Override
  public FileStatus[] listStatus(Path[] files, PathFilter filter) throws FileNotFoundException, IOException {
    return super.listStatus(files, filter);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws FileNotFoundException, IOException {
    return super.listLocatedStatus(f);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path p) throws FileNotFoundException, IOException {
    return super.listStatusIterator(p);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)
      throws FileNotFoundException, IOException {
    return super.listFiles(f, recursive);
  }

  @Override
  public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
    super.moveFromLocalFile(srcs, dst);
  }

  @Override
  public void moveFromLocalFile(Path src, Path dst) throws IOException {
    super.moveFromLocalFile(src, dst);
  }

  @Override
  public void moveToLocalFile(Path src, Path dst) throws IOException {
    super.moveToLocalFile(src, dst);
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    return super.mkdirs(f);
  }

  private FileStatus getFileStatus(PathContext context, FileStatus fileStatus) throws IOException {
    Path path = context.getOriginalPath(fileStatus.getPath());
    long length = fileStatus.getLen();
    boolean isdir = fileStatus.isDirectory();
    int block_replication = fileStatus.getReplication();
    long blocksize = fileStatus.getBlockSize();
    long modification_time = fileStatus.getModificationTime();
    long access_time = fileStatus.getAccessTime();
    FsPermission permission = fileStatus.getPermission();
    String owner = fileStatus.getOwner();
    String group = fileStatus.getGroup();
    return new FileStatus(length, isdir, block_replication, blocksize, modification_time, access_time, permission,
        owner, group, path);
  }

  private FileStatus[] getFileStatus(PathContext context, FileStatus[] listStatus) throws IOException {
    if (listStatus == null) {
      return null;
    }
    for (int i = 0; i < listStatus.length; i++) {
      listStatus[i] = getFileStatus(context, listStatus[i]);
    }
    return listStatus;
  }

  private boolean isSameFileSystem(FileSystem srcFs, FileSystem dstFs) {
    URI srcUri = srcFs.getUri();
    URI dstUri = dstFs.getUri();
    return srcUri.equals(dstUri);
  }

  private IOException handleError(IOException e, PathContext context, Path path) throws IOException {
    if (e instanceof FileNotFoundException) {
      return new FileNotFoundException(fixMessage(path, context.getOriginalPath(path), e.getMessage()));
    }
    return new IOException(fixMessage(path, context.getOriginalPath(path), e.getMessage()), e.getCause());
  }

  private String fixMessage(Path realPath, Path virtualPath, String message) {
    return message.replace(realPath.toString(), virtualPath.toString());
  }

}
