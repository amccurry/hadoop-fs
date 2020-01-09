package hadoop.fs.metadata;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public class MetaDataFileSystem extends FileSystem {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String DATA_KEYTAB_SUFFIX = ".data.keytab";
  private static final String DATA_PRINCIPAL_SUFFIX = ".data.principal";
  private static final String IO_FILE_BUFFER_SIZE = "io.file.buffer.size";
  private static final String USER_HOME_DIR_PREFIX = "/user/";
  private static final String DATA_PATH_SUFFIX = ".data.path";
  private static final String META_PATH_SUFFIX = ".meta.path";
  private static final Joiner PATH_JOINER = Joiner.on('/');
  private static final Splitter PATH_SPLITTER = Splitter.on('/');

  private final Logger LOGGER = LoggerFactory.getLogger(getClass());

  private URI _fsUri;
  private Path _workingDir;
  private Path _metaPath;
  private Path _dataPath;
  private String _authority;
  private String _rootMetaPath;
  private List<String> _rootMetaPathParts;
  private String _metaPathScheme;
  private String _metaPathAuthority;
  private UserGroupInformation _dataUgi;
  private String _configPrefix;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    setConf(conf);
    if (!uri.getScheme()
            .equals(getScheme())) {
      throw new IOException("uri " + uri + " does match filesystem scheme " + getScheme());
    }
    _fsUri = uri;
    _configPrefix = _fsUri.getScheme() + "." + _fsUri.getAuthority();
    _authority = uri.getAuthority();
    _metaPath = getQualifiedPathFromConf(conf, getConfigPrefix() + META_PATH_SUFFIX);
    _dataPath = getQualifiedPathFromConf(conf, getConfigPrefix() + DATA_PATH_SUFFIX);
    _rootMetaPath = _metaPath.toUri()
                             .getPath();
    _rootMetaPathParts = split(_rootMetaPath);
    _metaPathScheme = _metaPath.toUri()
                               .getScheme();
    _metaPathAuthority = _metaPath.toUri()
                                  .getAuthority();

    String principal = conf.get(getConfigPrefix() + DATA_PRINCIPAL_SUFFIX);
    if (principal != null) {
      String keytab = conf.get(getConfigPrefix() + DATA_KEYTAB_SUFFIX);
      _dataUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
    }
  }

  private String getConfigPrefix() {
    return _configPrefix;
  }

  /**
   * Gets data entry for given meta path;
   */
  protected DataEntry getDataEntry(Path metaPath) throws IOException {
    FileSystem metaFs = metaPath.getFileSystem(getConf());
    try (FSDataInputStream input = metaFs.open(metaPath)) {
      return OBJECT_MAPPER.readValue(input, DataEntry.class);
    }
  }

  /**
   * hdfs://something/meta/dir1/test1 => remote://auth/dir1/test1
   */
  protected Path getVirtualPath(Path metaPath) throws IOException {
    metaPath = makeQualifiedPath(metaPath);
    String fullMetaPath = metaPath.toUri()
                                  .getPath();
    String path = getVirtualPathStr(_rootMetaPath, fullMetaPath);
    return new Path(getScheme(), _authority, path);
  }

  /**
   * remote://auth/dir1/test1 => hdfs://something/meta/dir1/test1
   */
  protected MetaEntry getMetaEntry(Path virtualPath) throws IOException {
    virtualPath = makeQualifiedPath(virtualPath);
    URI virtualUri = virtualPath.toUri();
    if (!virtualUri.getScheme()
                   .equals(_fsUri.getScheme())) {
      throw new IOException("Wrong scheme for path " + virtualPath + " should be scheme " + _fsUri.getScheme());
    }
    if (!virtualUri.getAuthority()
                   .equals(_fsUri.getAuthority())) {
      throw new IOException(
          "Wrong authority for path " + virtualPath + " should be authority " + _fsUri.getAuthority());
    }
    String fullVirtualPath = virtualUri.getPath();
    List<String> fullVirtualPathParts = split(fullVirtualPath);
    Builder<String> builder = ImmutableList.builder();
    ImmutableList<String> list = builder.addAll(_rootMetaPathParts)
                                        .addAll(fullVirtualPathParts.subList(1, fullVirtualPathParts.size()))
                                        .build();
    String path = PATH_JOINER.join(list);
    Path metaPath = new Path(_metaPathScheme, _metaPathAuthority, path);
    return MetaEntry.builder()
                    .metaPath(metaPath)
                    .build();
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    LOGGER.info("open {} {}", f, bufferSize);
    try {
      MetaEntry metaEntry = getMetaEntry(f);
      DataEntry dataEntry = getDataEntry(metaEntry.getMetaPath());
      Path dataPath = dataEntry.getDataPath();
      FileSystem dataFs = dataPath.getFileSystem(getConf());
      FileStatus fileStatus = dataFs.getFileStatus(dataPath);
      if (fileStatus.getLen() == 0) {
        return new FSDataInputStream(new ReadNothing());
      }
      return dataFs.open(dataPath);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    try {
      MetaEntry metaEntry = getMetaEntry(f);
      Path metaPath = metaEntry.getMetaPath();
      Path dataPath = createDataPath(metaPath);
      storeDataPath(metaPath, dataPath, permission, overwrite);
      return createDataOutputStream(dataPath, bufferSize, progress);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new IOException("Not supported.");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    try {
      MetaEntry metaSrcEntry = getMetaEntry(src);
      Path metaSrcPath = metaSrcEntry.getMetaPath();
      MetaEntry metaDstEntry = getMetaEntry(dst);
      Path metaDstPath = metaDstEntry.getMetaPath();

      FileSystem srcmetaFs = metaSrcPath.getFileSystem(getConf());
      FileSystem dstmetaFs = metaDstPath.getFileSystem(getConf());
      if (!srcmetaFs.getUri()
                    .equals(dstmetaFs.getUri())) {
        return false;
      }
      return srcmetaFs.rename(metaSrcPath, metaDstPath);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    try {
      MetaEntry metaEntry = getMetaEntry(f);
      Path metaPath = metaEntry.getMetaPath();
      FileSystem metaFs = metaPath.getFileSystem(getConf());
      if (recursive) {
        return deleteRecursive(metaFs.getFileStatus(metaPath));
      } else {
        return deleteFile(metaFs, metaPath);
      }
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    try {
      MetaEntry metaEntry = getMetaEntry(f);
      Path metaPath = metaEntry.getMetaPath();
      FileSystem metaFs = metaPath.getFileSystem(getConf());
      FileStatus[] listStatus = metaFs.listStatus(metaPath);
      return fixFileStatusList(listStatus);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    try {
      MetaEntry metaEntry = getMetaEntry(f);
      Path metaPath = metaEntry.getMetaPath();
      FileSystem metaFs = metaPath.getFileSystem(getConf());
      return metaFs.mkdirs(metaPath, permission);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    LOGGER.info("getFileStatus {}", f);
    boolean logError = true;
    try {
      MetaEntry metaEntry = getMetaEntry(f);
      Path metaPath = metaEntry.getMetaPath();
      FileSystem metaFs = metaPath.getFileSystem(getConf());
      FileStatus fileStatus;
      try {
        LOGGER.info("getFileStatus from metafs {}", metaPath);
        fileStatus = metaFs.getFileStatus(metaPath);
      } catch (FileNotFoundException e) {
        LOGGER.info("getFileStatus FileNotFoundException {}", metaPath);
        logError = false;
        throw e;
      }
      return fixFileStatus(fileStatus);
    } catch (Throwable t) {
      if (logError) {
        LOGGER.error(t.getMessage(), t);
      }
      throw t;
    }
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    try {
      MetaEntry metaEntry = getMetaEntry(path);
      Path metaPath = metaEntry.getMetaPath();
      FileSystem metaFs = metaPath.getFileSystem(getConf());
      return metaFs.getAclStatus(metaPath);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    try {
      MetaEntry metaEntry = getMetaEntry(path);
      Path metaPath = metaEntry.getMetaPath();
      FileSystem metaFs = metaPath.getFileSystem(getConf());
      metaFs.modifyAclEntries(metaPath, aclSpec);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
    try {
      MetaEntry metaEntry = getMetaEntry(path);
      Path metaPath = metaEntry.getMetaPath();
      if (isFileLinkXAttrName(name)) {
        createLink(metaPath, value);
      } else {
        FileSystem metaFs = metaPath.getFileSystem(getConf());
        metaFs.setXAttr(metaPath, name, value, flag);
      }
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    try {
      MetaEntry metaEntry = getMetaEntry(path);
      Path metaPath = metaEntry.getMetaPath();
      FileSystem metaFs = metaPath.getFileSystem(getConf());
      return metaFs.getXAttr(metaPath, name);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    try {
      MetaEntry metaEntry = getMetaEntry(path);
      Path metaPath = metaEntry.getMetaPath();
      FileSystem metaFs = metaPath.getFileSystem(getConf());
      return metaFs.getXAttrs(metaPath);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
    try {
      MetaEntry metaEntry = getMetaEntry(path);
      Path metaPath = metaEntry.getMetaPath();
      FileSystem metaFs = metaPath.getFileSystem(getConf());
      return metaFs.getXAttrs(metaPath, names);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    try {
      MetaEntry metaEntry = getMetaEntry(path);
      Path metaPath = metaEntry.getMetaPath();
      FileSystem metaFs = metaPath.getFileSystem(getConf());
      return metaFs.listXAttrs(metaPath);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    try {
      MetaEntry metaEntry = getMetaEntry(path);
      Path metaPath = metaEntry.getMetaPath();
      FileSystem metaFs = metaPath.getFileSystem(getConf());
      metaFs.removeXAttr(metaPath, name);
    } catch (Throwable t) {
      LOGGER.error(t.getMessage(), t);
      throw t;
    }
  }

  @Override
  public Path getHomeDirectory() {
    try {
      return makeQualified(new Path(USER_HOME_DIR_PREFIX + UserGroupInformation.getCurrentUser()
                                                                               .getShortUserName()));
    } catch (IllegalArgumentException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    _workingDir = new_dir;
  }

  @Override
  public Path getWorkingDirectory() {
    return _workingDir;
  }

  @Override
  public String getScheme() {
    return "metadata";
  }

  @Override
  public URI getUri() {
    return _fsUri;
  }

  /**
   * Protected methods
   */

  /**
   * Create a new data path.
   */
  protected Path createDataPath(Path metaPath) {
    UUID uuid = UUID.randomUUID();
    long mostSigBits = uuid.getMostSignificantBits();
    long leastSigBits = uuid.getLeastSignificantBits();
    String path = digits(mostSigBits >> 32, 8) + "-" + digits(mostSigBits >> 16, 4) + "-" + digits(mostSigBits, 4) + "-"
        + digits(leastSigBits >> 48, 4) + "-" + digits(leastSigBits, 12);
    return new Path(_dataPath, path);
  }

  /**
   * Store the data path for the given meta path.
   */
  protected void storeDataPath(FSDataOutputStream output, DataEntry storageEntry) throws IOException {
    OBJECT_MAPPER.writeValue(output, storageEntry);
  }

  /**
   * Private methods
   */

  private FSDataOutputStream createDataOutputStream(Path dataPath, int bufferSize, Progressable progress)
      throws IOException {
    UserGroupInformation dataUgi = getDataUgi();
    try {
      return dataUgi.doAs((PrivilegedExceptionAction<FSDataOutputStream>) () -> {
        FileSystem dataFs = dataPath.getFileSystem(getConf());

        FsPermission dataPermission = FsCreateModes.applyUMask(FsPermission.getFileDefault(),
            FsPermission.getUMask(getConf()));
        short dataReplication = dataFs.getDefaultReplication(dataPath);
        long dataBlockSize = dataFs.getDefaultBlockSize(dataPath);
        return dataFs.create(dataPath, dataPermission, false, bufferSize, dataReplication, dataBlockSize, progress);
      });
    } catch (IOException e) {
      throw e;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private UserGroupInformation getDataUgi() throws IOException {
    if (_dataUgi == null) {
      return UserGroupInformation.getCurrentUser();
    }
    _dataUgi.checkTGTAndReloginFromKeytab();
    return _dataUgi;
  }

  class RemoteFSDataOutputStream extends FSDataOutputStream {

    private Closeable _trigger;

    public RemoteFSDataOutputStream(OutputStream out, Statistics stats, Closeable trigger) throws IOException {
      super(out, stats);
      _trigger = trigger;
    }

    @Override
    public void close() throws IOException {
      super.close();
      _trigger.close();
    }

  }

  private void createLink(Path metaPath, byte[] value) throws IOException {
    String pathStr = new String(value);
    Path dataPath = new Path(pathStr);
    FileSystem dataFs = dataPath.getFileSystem(getConf());
    dataPath = dataFs.makeQualified(dataPath);
    FileStatus fileStatus = dataFs.getFileStatus(dataPath);
    if (fileStatus == null) {
      throw new IOException("Data path " + dataPath + " does not exist.");
    }
    if (!fileStatus.isFile()) {
      throw new IOException("Data path " + dataPath + " is not a file.");
    }
    FileSystem metaFs = metaPath.getFileSystem(getConf());
    FileStatus metaFileStatus = metaFs.getFileStatus(metaPath);
    if (metaFileStatus == null) {
      throw new IOException("Meta path " + metaPath + " does not exist.");
    }
    if (!metaFileStatus.isFile()) {
      throw new IOException("Meta path " + metaPath + " is not a file.");
    }
    try (FSDataOutputStream output = metaFs.create(metaPath)) {
      String dataUri = dataPath.toUri()
                               .toString();
      DataEntry dataEntry = DataEntry.builder()
                                     .dataPathUri(dataUri)
                                     .managed(false)
                                     .build();
      storeDataPath(output, dataEntry);
    }
  }

  private boolean isFileLinkXAttrName(String name) {
    return name.equals(getScheme() + ".file.link");
  }

  private static String digits(long val, int digits) {
    long hi = 1L << (digits * 4);
    return Long.toHexString(hi | (val & (hi - 1)))
               .substring(1);
  }

  private boolean deleteRecursive(FileStatus metaFileStatus) throws IOException {
    if (metaFileStatus == null) {
      return false;
    }
    Path metaPath = metaFileStatus.getPath();
    FileSystem metaFs = metaPath.getFileSystem(getConf());
    if (metaFileStatus.isDirectory()) {
      FileStatus[] listStatus = metaFs.listStatus(metaFileStatus.getPath());
      if (listStatus == null) {
        return false;
      }
      boolean result = true;
      for (FileStatus status : listStatus) {
        if (!deleteRecursive(status)) {
          result = false;
        }
      }
      if (result) {
        return metaFs.delete(metaPath, false);
      }
      return result;
    } else {
      return deleteFile(metaFs, metaPath);
    }
  }

  private boolean deleteFile(FileSystem metaFs, Path metaPath) throws IOException {
    DataEntry storageEntry = getDataEntry(metaPath);
    Path dataPath = storageEntry.getDataPath();
    FileSystem dataFs = dataPath.getFileSystem(getConf());
    boolean result = metaFs.delete(metaPath, false);
    if (storageEntry.isManaged()) {
      if (result) {
        if (!dataFs.delete(dataPath, false)) {
          LOGGER.warn("Could not remove {}", dataPath);
        }
      }
    }
    return result;
  }

  private FileStatus[] fixFileStatusList(FileStatus[] listStatus) throws IOException {
    if (listStatus == null) {
      return null;
    }
    for (int i = 0; i < listStatus.length; i++) {
      listStatus[i] = fixFileStatus(listStatus[i]);
    }
    return listStatus;
  }

  private FileStatus fixFileStatus(FileStatus metaFileStatus) throws IOException {
    if (metaFileStatus == null) {
      return null;
    }
    Path metaPath = metaFileStatus.getPath();
    long length;
    if (!metaFileStatus.isDirectory()) {
      DataEntry dataEntry = getDataEntry(metaPath);
      Path dataPath = dataEntry.getDataPath();
      FileSystem dataFs = dataPath.getFileSystem(getConf());
      try {
        FileStatus dataFileStatus = dataFs.getFileStatus(dataPath);
        length = dataFileStatus.getLen();
      } catch (FileNotFoundException e) {
        LOGGER.warn("DataPath {} not found using 0 length.", dataPath);
        length = 0;
      }
    } else {
      length = metaFileStatus.getLen();
    }

    Path path = getVirtualPath(metaPath);
    boolean isdir = metaFileStatus.isDirectory();
    int block_replication = metaFileStatus.getReplication();
    long blocksize = metaFileStatus.getBlockSize();
    long modification_time = metaFileStatus.getModificationTime();
    long access_time = metaFileStatus.getAccessTime();
    FsPermission permission = metaFileStatus.getPermission();
    String owner = metaFileStatus.getOwner();
    String group = metaFileStatus.getGroup();

    return new FileStatus(length, isdir, block_replication, blocksize, modification_time, access_time, permission,
        owner, group, path);
  }

  private void storeDataPath(Path metaPath, Path dataPath, FsPermission permission, boolean overwrite)
      throws IOException {
    FileSystem metaFs = metaPath.getFileSystem(getConf());
    short metaReplication = metaFs.getDefaultReplication(dataPath);
    long metaBlockSize = metaFs.getDefaultBlockSize(dataPath);

    int bufferSize = metaFs.getConf()
                           .getInt(IO_FILE_BUFFER_SIZE, 4096);
    try (FSDataOutputStream output = metaFs.create(metaPath, permission, overwrite, bufferSize, metaReplication,
        metaBlockSize, null)) {
      String dataUri = dataPath.toUri()
                               .toString();
      DataEntry dataEntry = DataEntry.builder()
                                     .managementId(UUID.randomUUID()
                                                       .toString())
                                     .dataPathUri(dataUri)
                                     .managed(true)
                                     .build();
      storeDataPath(output, dataEntry);
    }
  }

  private Path getQualifiedPathFromConf(Configuration conf, String propertyName) throws IOException {
    String pathStr = conf.get(propertyName);
    if (pathStr == null) {
      throw new IOException("Property missing " + propertyName);
    }
    return makeQualifiedPath(new Path(pathStr));
  }

  private Path makeQualifiedPath(Path p) throws IOException {
    FileSystem fs = p.getFileSystem(getConf());
    return fs.makeQualified(p);
  }

  private String getVirtualPathStr(String rootMetaPath, String fullMetaPath) throws IOException {
    List<String> fullMetaPathParts = split(fullMetaPath);
    if (!startsWith(_rootMetaPathParts, fullMetaPathParts)) {
      throw new IOException("Meta path " + fullMetaPath + " does not start with root meta path " + rootMetaPath);
    }

    List<String> list = fullMetaPathParts.subList(_rootMetaPathParts.size(), fullMetaPathParts.size());
    return '/' + PATH_JOINER.join(list);
  }

  private boolean startsWith(List<String> prefixList, List<String> fullList) {
    if (prefixList.size() > fullList.size()) {
      return false;
    }
    for (int i = 0; i < prefixList.size(); i++) {
      String element = fullList.get(i);
      if (element == null) {
        return false;
      }
      if (!element.equals(prefixList.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static List<String> split(String path) {
    return ImmutableList.copyOf(PATH_SPLITTER.split(path));
  }

  /**
   * FileSystem methods that do not need to be modified
   */

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    return super.create(f);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
    return super.create(f, overwrite);
  }

  @Override
  public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
    return super.create(f, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, short replication) throws IOException {
    return super.create(f, replication);
  }

  @Override
  public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
    return super.create(f, replication, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
    return super.create(f, overwrite, bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress)
      throws IOException {
    return super.create(f, overwrite, bufferSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
      throws IOException {
    return super.create(f, overwrite, bufferSize, replication, blockSize);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return super.create(f, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    return super.create(f, permission, flags, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress, ChecksumOpt checksumOpt) throws IOException {
    return super.create(f, permission, flags, bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  public FSDataOutputStream append(Path f) throws IOException {
    return super.append(f);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    return super.append(f, bufferSize);
  }

  @Override
  public void copyFromLocalFile(Path src, Path dst) throws IOException {
    super.copyFromLocalFile(src, dst);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    super.copyFromLocalFile(delSrc, src, dst);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
    super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
    super.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  @Override
  public void copyToLocalFile(Path src, Path dst) throws IOException {
    super.copyToLocalFile(src, dst);
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    super.copyToLocalFile(delSrc, src, dst);
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem) throws IOException {
    super.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
  }

  @SuppressWarnings("deprecation")
  @Override
  public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    return super.createNonRecursive(f, overwrite, bufferSize, replication, blockSize, progress);
  }

  @SuppressWarnings("deprecation")
  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    return super.createNonRecursive(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @SuppressWarnings("deprecation")
  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return super.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress);
  }

  @Override
  public boolean createNewFile(Path f) throws IOException {
    return super.createNewFile(f);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    return super.exists(f);
  }

  @SuppressWarnings("deprecation")
  @Override
  public long getBlockSize(Path f) throws IOException {
    return super.getBlockSize(f);
  }

  @SuppressWarnings("deprecation")
  @Override
  public long getDefaultBlockSize() {
    return super.getDefaultBlockSize();
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    return super.getDefaultBlockSize(f);
  }

  @SuppressWarnings("deprecation")
  @Override
  public short getDefaultReplication() {
    return super.getDefaultReplication();
  }

  @Override
  public short getDefaultReplication(Path path) {
    return super.getDefaultReplication(path);
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    return super.getContentSummary(f);
  }

  @Override
  public Path makeQualified(Path path) {
    return super.makeQualified(path);
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    return super.mkdirs(f);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value) throws IOException {
    super.setXAttr(path, name, value);
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    return super.isDirectory(f);
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    return super.isFile(f);
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return super.globStatus(pathPattern);
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    return super.globStatus(pathPattern, filter);
  }

}
