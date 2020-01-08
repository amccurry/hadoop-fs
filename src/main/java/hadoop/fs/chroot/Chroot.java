package hadoop.fs.chroot;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public interface Chroot {

  default void initialize(Configuration conf, ChrootFileSystem chrootFileSystem) throws IOException {

  }

  default Path getRealPath(Path chrootPath) throws IOException {
    throw new IOException("Not implemented");
  }

  default Path getChrootPath(Path realPath) throws IOException {
    throw new IOException("Not implemented");
  }

}
