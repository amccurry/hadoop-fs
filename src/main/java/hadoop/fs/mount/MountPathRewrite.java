package hadoop.fs.mount;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;

import hadoop.fs.util.FsUtil;

public class MountPathRewrite implements Mount {

  private final Path _dstPath;
  private final Path _srcPath;
  private final List<String> _dstParts;
  private final List<String> _srcParts;
  private final MountKey _mountKey;

  public MountPathRewrite(Path srcPath, Path dstPath) {
    _srcPath = srcPath;
    _dstPath = dstPath;
    _dstParts = FsUtil.pathSplit(_dstPath.toUri()
                                         .getPath());
    _srcParts = FsUtil.pathSplit(_srcPath.toUri()
                                         .getPath());
    _mountKey = MountKey.create(dstPath);
  }

  public MountKey getMountKey() {
    return _mountKey;
  }

  public Path toMountPath(Path path) throws IOException {
    checkDstPath(path);
    String pathStr = path.toUri()
                         .getPath();
    List<String> partParts = FsUtil.pathSplit(pathStr);
    List<String> differenceParts = partParts.subList(_dstParts.size(), partParts.size());
    if (differenceParts.size() == 0) {
      return _srcPath;
    }
    String differencePath = FsUtil.PATH_JOINER.join(differenceParts);
    return new Path(_srcPath, differencePath);
  }

  public Path fromMountPath(Path path) throws IOException {
    checkSrcPath(path);
    String pathStr = path.toUri()
                         .getPath();
    List<String> partParts = FsUtil.pathSplit(pathStr);
    List<String> differenceParts = partParts.subList(_srcParts.size(), partParts.size());
    if (differenceParts.size() == 0) {
      return _dstPath;
    }
    String differencePath = FsUtil.PATH_JOINER.join(differenceParts);
    return new Path(_dstPath, differencePath);
  }

  private void checkSrcPath(Path path) throws IOException {
    if (!checkPath(_srcPath, path)) {
      throw new IOException("Path " + path + " is not in src path " + _srcPath);
    }
  }

  private void checkDstPath(Path path) throws IOException {
    if (!checkPath(_dstPath, path)) {
      throw new IOException("Path " + path + " is not in dst path " + _dstPath);
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

}
