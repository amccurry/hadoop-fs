package hadoop.fs.mount;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class MountFileSystem extends FileSystem {

  private static final String INFO_PATH = ".info.path";
  private Path _workingDir;
  private URI _uri;
  private Path _infoPath;

  @Override
  public URI getUri() {
    return _uri;
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    _uri = name;
    String authority = _uri.getAuthority();
    String infoStr = conf.get(getScheme() + "." + authority + INFO_PATH);
    Path infoPath = new Path(infoStr);
    FileSystem fileSystem = infoPath.getFileSystem(getConf());
    _infoPath = fileSystem.makeQualified(infoPath);
  }

  private Mount getMount(Path path) {
    return null;
  }

  @Override
  public String getScheme() {
    return "mount";
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    Path path = toMountPath(f);
    FileSystem fileSystem = path.getFileSystem(getConf());
    return fileSystem.open(path, bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    Path path = toMountPath(f);
    FileSystem fileSystem = path.getFileSystem(getConf());
    return fileSystem.create(path, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    Path path = toMountPath(f);
    FileSystem fileSystem = path.getFileSystem(getConf());
    return fileSystem.append(path, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Path srcPath = toMountPath(src);
    Path dstPath = toMountPath(dst);

    FileSystem srcFileSystem = srcPath.getFileSystem(getConf());
    FileSystem dstFileSystem = srcPath.getFileSystem(getConf());

    if (!isSameFileSystem(srcFileSystem, dstFileSystem)) {
      return false;
    }
    return srcFileSystem.rename(srcPath, dstPath);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    Path path = toMountPath(f);
    FileSystem fileSystem = path.getFileSystem(getConf());
    return fileSystem.delete(path, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    Mount mount = getMount(f);
    Path path = mount.toMountPath(f);
    FileSystem fileSystem = path.getFileSystem(getConf());
    return fromMountPath(mount, fileSystem.listStatus(path));
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    Path path = toMountPath(f);
    FileSystem fileSystem = path.getFileSystem(getConf());
    return fileSystem.mkdirs(path, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Mount mount = getMount(f);
    Path path = mount.toMountPath(f);
    FileSystem fileSystem = path.getFileSystem(getConf());
    return fromMountPath(mount, fileSystem.getFileStatus(path));
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    _workingDir = new_dir;
  }

  @Override
  public Path getWorkingDirectory() {
    return _workingDir;
  }

  private Path toMountPath(Path path) throws IOException {
    return getMount(path).toMountPath(path);
  }

  private FileStatus fromMountPath(Mount mount, FileStatus fileStatus) throws IOException {
    Path path = mount.fromMountPath(fileStatus.getPath());
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

  private FileStatus[] fromMountPath(Mount mount, FileStatus[] listStatus) throws IOException {
    if (listStatus == null) {
      return null;
    }
    for (int i = 0; i < listStatus.length; i++) {
      listStatus[i] = fromMountPath(mount, listStatus[i]);
    }
    return listStatus;
  }

  private boolean isSameFileSystem(FileSystem srcFs, FileSystem dstFs) {
    URI srcUri = srcFs.getUri();
    URI dstUri = dstFs.getUri();
    return srcUri.equals(dstUri);
  }

}
