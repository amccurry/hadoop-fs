package hadoop.fs.mount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MountManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(MountManager.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String MOUNT_FACTORY_CLASS = ".mount.factory.class";
  private static final String DATE_PATTERN = "YYYYMMddHHmmss";
  private static final String MOUNT_RELOAD = "mount-reload/";
  private static final String PATH_SUFFIX = ".path";
  private static final String PERIOD_SUFFIX = ".period";
  private static final String DELAY_SUFFIX = ".delay";
  private static final String AUTOMATIC_UPDATES_DISABLED_SUFFIX = ".automatic.updates.disabled";
  private static final String DEFAULT_MOUNT_SUFFIX = ".default.mount";

  private static final Map<String, MountManager> INSTANCES = new ConcurrentHashMap<>();

  public synchronized static MountManager getInstance(Configuration conf, String configPrefix, Path rootVirtualPath)
      throws IOException {
    MountManager mountCache = INSTANCES.get(configPrefix);
    if (mountCache == null) {
      INSTANCES.put(configPrefix, mountCache = new MountManager(conf, configPrefix, rootVirtualPath));
    }
    return mountCache;
  }

  private final AtomicReference<Map<MountKey, MountPathRewrite>> _reloadingMountsRef = new AtomicReference<>(
      new ConcurrentHashMap<>());
  private final Map<MountKey, MountPathRewrite> _mounts = new ConcurrentHashMap<>();
  private final Timer _timer;
  private final Path _mountPath;
  private final Configuration _conf;
  private final MountPathRewrite _defaultMount;
  private final MountFactory _mountFactory;

  private MountManager(Configuration conf, String configPrefix, Path rootVirtualPath) throws IOException {
    _timer = new Timer(MOUNT_RELOAD + configPrefix, true);
    _conf = conf;

    _mountFactory = getMountFactory(conf, configPrefix);
    _mountFactory.initialize();
    String defaultRealMount = conf.get(configPrefix + DEFAULT_MOUNT_SUFFIX);
    Path defaultRealMountPath = makeQualified(conf, new Path(defaultRealMount));
    _defaultMount = new MountPathRewrite(defaultRealMountPath, rootVirtualPath);
    boolean disableAutomaticUpdates = _conf.getBoolean(configPrefix + AUTOMATIC_UPDATES_DISABLED_SUFFIX, false);
    long delay = _conf.getLong(configPrefix + DELAY_SUFFIX, TimeUnit.SECONDS.toMillis(1));
    long period = _conf.getLong(configPrefix + PERIOD_SUFFIX, TimeUnit.MINUTES.toMillis(5));
    if (!disableAutomaticUpdates) {
      String configName = configPrefix + PATH_SUFFIX;
      String mountPathStr = _conf.get(configName);
      if (mountPathStr == null) {
        throw new IllegalArgumentException("Config param " + configName + " missing");
      }
      Path mountPath = new Path(mountPathStr);
      FileSystem fileSystem = mountPath.getFileSystem(conf);
      _mountPath = fileSystem.makeQualified(mountPath);
      _timer.schedule(getTimerTask(), delay, period);
    } else {
      _mountPath = null;
    }
  }

  public Mount getMount(Path path) {
    MountKey mountKey = MountKey.create(path);
    Mount mount = findMountFromInMemoryMounts(mountKey);
    if (mount == null) {
      mount = findMountFromFileMounts(mountKey);
      if (mount == null) {
        mount = findMountFromFactory(mountKey);
        if (mount == null) {
          mount = _defaultMount;
        }
      }
    }
    return mount;
  }

  private Mount findMountFromFactory(MountKey mountKey) {
    do {
      Mount mount = _mountFactory.findMount(mountKey);
      if (mount != null) {
        return mount;
      }
    } while ((mountKey = mountKey.getParentKey()) != null);
    return null;
  }

  private Mount findMountFromFileMounts(MountKey mountKey) {
    return findMount(_reloadingMountsRef.get(), mountKey);
  }

  private Mount findMountFromInMemoryMounts(MountKey mountKey) {
    return findMount(_mounts, mountKey);
  }

  public void addMount(Path realPath, Path virtualPath, boolean persistant) throws IOException {
    MountPathRewrite mountPathRewrite = new MountPathRewrite(realPath, virtualPath);
    _mounts.put(mountPathRewrite.getMountKey(), mountPathRewrite);
    if (persistant) {
      MountEntry mountEntry = MountEntry.builder()
                                        .realPath(realPath.toString())
                                        .virtualPath(virtualPath.toString())
                                        .build();
      String newEntry = OBJECT_MAPPER.writeValueAsString(mountEntry);
      FileSystem fileSystem = _mountPath.getFileSystem(_conf);
      Path newFile = new Path("/tmp/" + UUID.randomUUID()
                                            .toString());
      try (PrintWriter pw = new PrintWriter(fileSystem.create(newFile))) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(_mountPath)))) {
          String line;
          while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (!line.isEmpty()) {
              pw.println(line);
            }
          }
        }
        pw.println(newEntry);
      }

      // Backup existing file to old file. Then try to prompt new file. If
      // successful then delete old file, if not then put old file back and
      // delete the new file.
      Path oldFile = new Path(_mountPath.getParent(), _mountPath.getName() + ".old." + now());
      if (fileSystem.rename(_mountPath, oldFile)) {
        if (fileSystem.rename(newFile, _mountPath)) {
          fileSystem.delete(oldFile, false);
        } else {
          fileSystem.rename(oldFile, newFile);
        }
      }
    }
  }

  public void reloadMounts() throws IOException {
    FileSystem fileSystem = _mountPath.getFileSystem(_conf);
    if (fileSystem.exists(_mountPath)) {
      loadCache(fileSystem);
    }
  }

  private String now() {
    return new SimpleDateFormat(DATE_PATTERN).format(new Date());
  }

  private Path makeQualified(Configuration conf, Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(conf);
    return fileSystem.makeQualified(path);
  }

  private void loadCache(FileSystem fileSystem) throws IOException, JsonParseException, JsonMappingException {
    _reloadingMountsRef.set(loadFromFile());
  }

  private ConcurrentMap<MountKey, MountPathRewrite> loadFromFile() throws IOException {
    ConcurrentMap<MountKey, MountPathRewrite> mounts = new ConcurrentHashMap<>();
    FileSystem fileSystem = _mountPath.getFileSystem(_conf);
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(_mountPath)))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.trim()
                 .isEmpty()) {
          parseLine(mounts, line);
        }
      }
    }
    return mounts;
  }

  private void parseLine(ConcurrentMap<MountKey, MountPathRewrite> mounts, String line) throws IOException {
    MountEntry mountEntry = OBJECT_MAPPER.readValue(line, MountEntry.class);
    MountPathRewrite mountPathRewrite = new MountPathRewrite(mountEntry.getRealPath(), mountEntry.getVirtualPath());
    mounts.put(mountPathRewrite.getMountKey(), mountPathRewrite);
  }

  private Mount findMount(Map<MountKey, MountPathRewrite> mounts, MountKey mountKey) {
    do {
      Mount mount = mounts.get(mountKey);
      if (mount != null) {
        return mount;
      }
    } while ((mountKey = mountKey.getParentKey()) != null);
    return null;
  }

  private TimerTask getTimerTask() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          reloadMounts();
        } catch (Exception e) {
          LOGGER.error("Unknown error", e);
        }
      }
    };
  }

  private MountFactory getMountFactory(Configuration conf, String configPrefix) {
    List<MountFactory> list = conf.getInstances(configPrefix + MOUNT_FACTORY_CLASS, MountFactory.class);
    if (list == null || list.isEmpty()) {
      return MountFactory.DEFAULT;
    } else {
      return list.get(0);
    }
  }

}
