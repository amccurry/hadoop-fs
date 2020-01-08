package hadoop.fs.chroot;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BasicChroot extends Configured implements Chroot {

  private static final String FS = ".fs";

  private Path _real;
  private String _realPath;
  private URI _chrootUri;
  private FileSystem _realFileSystem;

  @Override
  public void initialize(Configuration conf, ChrootFileSystem chrootFileSystem) throws IOException {
    _chrootUri = chrootFileSystem.getUri();
    String basePathStr = conf.get(chrootFileSystem.getConfigPrefix() + FS);
    Path path = new Path(basePathStr);
    _realFileSystem = path.getFileSystem(getConf());
    _real = _realFileSystem.makeQualified(path);
    URI uri = _real.toUri();
    _realPath = uri.getPath();
  }

  @Override
  public Path getRealPath(Path chrootPath) throws IOException {
    URI uri = chrootPath.toUri();
    String path = uri.getPath();
    if (path.length() == 0) {
      path = "/";
    }
    if (path.equals("/")) {
      return _real;
    }
    return new Path(_real, path.substring(1));
  }

  @Override
  public Path getChrootPath(Path realPath) throws IOException {
    checkRealPathUri(realPath);
    String path = realPath.toUri()
                          .getPath();
    String pathStr = path.substring(_realPath.length());
    if (pathStr.isEmpty()) {
      pathStr = "/";
    }
    return new Path(_chrootUri.getScheme(), _chrootUri.getAuthority(), pathStr);
  }

  private void checkRealPathUri(Path realPath) throws IOException {
    FileSystem fileSystem = realPath.getFileSystem(getConf());
    if (!fileSystem.getUri()
                   .equals(_realFileSystem.getUri())) {
      throw new IllegalArgumentException(
          "Wrong filesystem for " + realPath + ", expected " + _realFileSystem + " was " + fileSystem);
    }
    URI uri = realPath.toUri();
    if (!uri.getPath()
            .startsWith(_realPath)) {
      throw new IllegalArgumentException("Wrong path, expected " + _realPath + "/* was " + uri.getPath());
    }
  }

}
