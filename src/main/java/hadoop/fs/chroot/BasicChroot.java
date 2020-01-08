package hadoop.fs.chroot;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BasicChroot extends Configured implements Chroot {

  private static final String FS = ".fs";

  private Path _chrootRoot;
  private Path _real;
  private URI _chrootFileSystemUri;
  private URI _realFileSystemUri;

  @Override
  public void initialize(Configuration conf, ChrootFileSystem chrootFileSystem) throws IOException {
    Path path = new Path(conf.get(chrootFileSystem.getConfigPrefix() + FS));
    FileSystem fileSystem = path.getFileSystem(getConf());
    path = fileSystem.makeQualified(path);
    initialize(chrootFileSystem.getUri(), fileSystem.getUri(), path);
  }

  void initialize(URI chrootFileSystemUri, URI realFileSystemUri, Path realQualified) throws IOException {
    _chrootFileSystemUri = chrootFileSystemUri;
    _realFileSystemUri = realFileSystemUri;
    _chrootRoot = new Path(_chrootFileSystemUri.getScheme(), _chrootFileSystemUri.getAuthority(), "/");
    _real = realQualified;
  }

  @Override
  public Path getRealPath(Path chrootPath) throws IOException {
    URI uri = chrootPath.toUri();
    String path = uri.getPath();
    if (path.length() == 0) {
      path = "/";
    }
    if (path.equals("/")) {
      return getRealPath();
    }
    return new Path(getRealPath(), path.substring(1));
  }

  @Override
  public Path getChrootPath(Path realPath) throws IOException {
    checkRealPathUri(realPath);
    String path = realPath.toUri()
                          .getPath();
    String pathStr = path.substring(getRealPathStr().length());
    if (pathStr.isEmpty()) {
      pathStr = "/";
    }
    return new Path(_chrootRoot, pathStr);
  }

  private void checkRealPathUri(Path realPath) throws IOException {
    URI uri = realPath.toUri();
    if (!equals(uri.getScheme(), _realFileSystemUri.getScheme())) {
      throw new IllegalArgumentException(
          "Wrong scheme for " + realPath + ", expected " + _realFileSystemUri + " was " + _realFileSystemUri);
    }

    if (!equals(uri.getAuthority(), _realFileSystemUri.getAuthority())) {
      throw new IllegalArgumentException(
          "Wrong authority for " + realPath + ", expected " + _realFileSystemUri + " was " + _realFileSystemUri);
    }

    if (!uri.getPath()
            .startsWith(getRealPathStr())) {
      throw new IllegalArgumentException("Wrong path, expected " + getRealPathStr() + "/* was " + uri.getPath());
    }
  }

  private boolean equals(String s1, String s2) {
    if (s1 == s2) {
      return true;
    } else if (s1 != null) {
      return s1.equals(s2);
    } else {
      return false;
    }
  }

  private String getRealPathStr() throws IOException {
    return getRealPath().toUri()
                        .getPath();
  }

  protected Path getRealPath() throws IOException {
    return _real;
  }

}
