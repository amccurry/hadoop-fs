package hadoop.fs.mount;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;

import hadoop.fs.util.FsUtil;

public class MountPathRewrite implements Mount {

  private final Path _virtualPath;
  private final Path _realPath;
  private final List<String> _virtualParts;
  private final List<String> _realParts;
  private final MountKey _mountKey;

  public MountPathRewrite(Path realPath, Path virtualPath) {
    _realPath = realPath;
    _virtualPath = virtualPath;
    _virtualParts = FsUtil.getPathParts(_virtualPath);
    _realParts = FsUtil.getPathParts(_realPath);
    _mountKey = MountKey.create(virtualPath);
  }

  public MountKey getMountKey() {
    return _mountKey;
  }

  public Path toMountPath(Path path) throws IOException {
    checkVirtualPath(path);
    String pathStr = path.toUri()
                         .getPath();
    List<String> partParts = FsUtil.getPathParts(pathStr);
    List<String> differenceParts = partParts.subList(_virtualParts.size(), partParts.size());
    if (differenceParts.size() == 0) {
      return _realPath;
    }
    String differencePath = FsUtil.PATH_JOINER.join(differenceParts);
    return new Path(_realPath, differencePath);
  }

  public Path fromMountPath(Path path) throws IOException {
    checkRealPath(path);
    String pathStr = path.toUri()
                         .getPath();
    List<String> partParts = FsUtil.getPathParts(pathStr);
    List<String> differenceParts = partParts.subList(_realParts.size(), partParts.size());
    if (differenceParts.size() == 0) {
      return _virtualPath;
    }
    String differencePath = FsUtil.PATH_JOINER.join(differenceParts);
    return new Path(_virtualPath, differencePath);
  }

  private void checkRealPath(Path path) throws IOException {
    if (!checkPath(_realPath, path)) {
      throw new IOException("Path " + path + " is not in real path " + _realPath);
    }
  }

  private void checkVirtualPath(Path path) throws IOException {
    if (!checkPath(_virtualPath, path)) {
      throw new IOException("Path " + path + " is not in virtual path " + _virtualPath);
    }
  }

  private boolean checkPath(Path mountPath, Path path) {
    // mount path must be a parent of path
    do {
      if (mountPath.equals(path)) {
        return true;
      }
    } while ((path = path.getParent()) != null);
    return false;
  }

  @Override
  public String toString() {
    return "MountPathRewrite [virtualPath=" + _virtualPath + ", realPath=" + _realPath + "]";
  }

}
