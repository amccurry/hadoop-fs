package hadoop.fs.mount;

import java.net.URI;
import java.util.List;

import org.apache.hadoop.fs.Path;

import hadoop.fs.util.FsUtil;
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
public class MountKey {

  String scheme;

  String authority;

  List<String> pathParts;

  public static MountKey create(Path path) {
    URI uri = path.toUri();
    return MountKey.builder()
                   .authority(uri.getAuthority())
                   .scheme(uri.getScheme())
                   .pathParts(FsUtil.pathSplit(uri.getPath()))
                   .build();
  }

  public MountKey getParentKey() {
    List<String> parts = getPathParts();
    if (parts == null || parts.size() <= 1) {
      return null;
    }
    return toBuilder().pathParts(parts.subList(0, parts.size() - 1))
                      .build();
  }

}
