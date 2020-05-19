package hadoop.fs.cache;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class S3AFSCacheFileSystem extends FSCacheFileSystem {

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    _cacheFsUri = name;
    Path path = new Path("s3a://" + _cacheFsUri.getAuthority() + "/");
    FileSystem fileSystem = path.getFileSystem(conf);
    _realFsUri = fileSystem.getUri();
  }

  @Override
  public String getScheme() {
    return "cache-s3a";
  }

}
