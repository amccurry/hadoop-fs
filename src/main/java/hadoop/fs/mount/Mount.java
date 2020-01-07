package hadoop.fs.mount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public interface Mount {

  public static Mount NO_OP = new Mount() {
  };

  default Path toMountPath(Path path) throws IOException {
    return path;
  }

  default Path fromMountPath(Path path) throws IOException {
    return path;
  }

}