package hadoop.fs.base;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ContextFileSystem extends FileSystem {

  private static final String USER_HOME_DIR_PREFIX = "/user/";

  private final Logger LOGGER = LoggerFactory.getLogger(getClass());
  private Path _workingDir;

  protected abstract PathContext getPathContext(Path f) throws IOException;

  public String getConfigPrefix() {
    return getScheme() + "." + getUri().getAuthority();
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
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

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
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

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
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

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
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

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
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

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
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

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
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

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
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
