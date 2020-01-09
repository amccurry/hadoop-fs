package hadoop.fs.metadata;

import java.io.IOException;
import java.io.InputStream;

public class ReadNothing extends InputStream {

  @Override
  public int read() throws IOException {
    return -1;
  }

}
