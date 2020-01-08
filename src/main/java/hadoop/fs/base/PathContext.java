package hadoop.fs.base;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public interface PathContext {

  Path getContextPath() throws IOException;

  Path getOriginalPath() throws IOException;

  Path getOriginalPath(Path contextPath) throws IOException;

}
