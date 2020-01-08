package hadoop.fs.util;

import java.util.List;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

public class FsUtil {

  public static final Joiner PATH_JOINER = Joiner.on('/');
  public static final Splitter PATH_SPLITTER = Splitter.on('/');

  public static List<String> pathSplit(String s) {
    return ImmutableList.copyOf(PATH_SPLITTER.split(s));
  }

  public static boolean isRoot(String pathStr) {
    return pathStr.equals("/");
  }

  public static List<String> getPathParts(Path path) {
    return getPathParts(path.toUri()
                            .getPath());
  }

  public static List<String> getPathParts(String pathStr) {
    if (FsUtil.isRoot(pathStr)) {
      return ImmutableList.of("");
    }
    return FsUtil.pathSplit(pathStr);
  }
}
