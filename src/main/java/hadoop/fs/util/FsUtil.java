package hadoop.fs.util;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

public class FsUtil {

  public static final Joiner PATH_JOINER = Joiner.on('/');
  public static final Splitter PATH_SPLITTER = Splitter.on('/');

  public static List<String> pathSplit(String s) {
    return ImmutableList.copyOf(PATH_SPLITTER.split(s));
  }

}
