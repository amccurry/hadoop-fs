package hadoop.fs.cache;

import java.io.IOException;

import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;

public class FSCachedInputStream extends FSInputStream implements CanSetReadahead {

  private final FSDataInputStream _input;
  private final FSCache _fsCache;
  private final FileStatus _fileStatus;

  public FSCachedInputStream(FSCache fsCache, FileStatus fileStatus, FSDataInputStream fsDataInputStream) {
    _fileStatus = fileStatus;
    _fsCache = fsCache;
    _input = fsDataInputStream;
  }

  @Override
  public void setReadahead(Long readahead) throws IOException, UnsupportedOperationException {
    _input.setReadahead(readahead);
  }

  @Override
  public void seek(long pos) throws IOException {
    _input.seek(pos);
  }

  @Override
  public long getPos() throws IOException {
    return _input.getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return _input.seekToNewSource(targetPos);
  }

  @Override
  public int read() throws IOException {
    return _input.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return _fsCache.read(_fileStatus, _input, b, off, len);
  }

}