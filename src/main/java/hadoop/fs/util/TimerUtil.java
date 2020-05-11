package hadoop.fs.util;

import java.util.Arrays;

import org.slf4j.Logger;

public class TimerUtil {

  public static TimerCloseable time(Logger logger, String label, String... args) {
    final long start = System.nanoTime();
    return new TimerCloseable() {

      @Override
      public void close() {
        final long end = System.nanoTime();
        if (args != null) {
          logger.info("{} {} took {} ms", label, Arrays.asList(args), (end - start) / 1000000.0);
        } else {
          logger.info("{} took {} ms", label, (end - start) / 1000000.0);
        }
      }
    };
  }

}
