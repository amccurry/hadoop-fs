package hadoop.fs.util;

import java.io.Closeable;

public interface TimerCloseable extends Closeable {

  @Override
  public void close();

}
