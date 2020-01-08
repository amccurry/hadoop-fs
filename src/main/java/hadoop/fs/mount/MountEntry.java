package hadoop.fs.mount;

import org.apache.hadoop.fs.Path;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Value;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@Builder(toBuilder = true)
@EqualsAndHashCode
public class MountEntry {

  String realPath;

  String virtualPath;

  public Path getRealPath() {
    return new Path(realPath);
  }

  public Path getVirtualPath() {
    return new Path(virtualPath);
  }
}
