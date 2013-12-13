package org.apache.hadoop.fs.WTF;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * A block-based {@link FileSystem} backed by
 * wtf.
 * </p>
 */
public class WTFFileSystem extends FileSystem {

  private Path workingDir;

  public WTFFileSystem() {
    // set store in initialize()
  }
  
  public WTFFileSystem(FileSystemStore store) {
      this.store = store;
  }

  private Path makeAbsolute(Path path) {
      if (path.isAbsolute()) {
          return path;
      }
      return new Path(workingDir, path);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    if (store == null) {
      store = createDefaultStore(conf);
    }
    store.initialize(uri, conf);
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());    
    this.workingDir =
      new Path("/user", System.getProperty("user.name")).makeQualified(this);
  }  

  @Override
  public String getName() {
    return getUri().toString();
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    workingDir = makeAbsolute(dir);
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    Path absolutePath = makeAbsolute(path);
    List<Path> paths = new ArrayList<Path>();
    do {
      paths.add(0, absolutePath);
      absolutePath = absolutePath.getParent();
    } while (absolutePath != null);
    
    boolean result = true;
    for (Path p : paths) {
      result &= mkdir(p);
    }
    return result;
  }
  
  @Override
  public boolean isFile(Path path) throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(path));
    if (inode == null) {
      return false;
    }
    return inode.isFile();
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Path absolutePath = makeAbsolute(f);
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      return null;
    }
    if (inode.isFile()) {
      return new FileStatus[] {
        new WTFFileStatus(f.makeQualified(this), inode)
      };
    }
    ArrayList<FileStatus> ret = new ArrayList<FileStatus>();
    for (Path p : store.listSubPaths(absolutePath)) {
      ret.add(getFileStatus(p.makeQualified(this)));
    }
    return ret.toArray(new FileStatus[0]);
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public FSDataOutputStream create(Path file, FsPermission permission,
      boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress)
    throws IOException {

    INode inode = store.retrieveINode(makeAbsolute(file));
    if (inode != null) {
      if (overwrite) {
        delete(file);
      } else {
        throw new IOException("File already exists: " + file);
      }
    } else {
      Path parent = file.getParent();
      if (parent != null) {
        if (!mkdirs(parent)) {
          throw new IOException("Mkdirs failed to create " + parent.toString());
        }
      }      
    }
    return new FSDataOutputStream
        (new WTFOutputStream(getConf(), store, makeAbsolute(file),
                            blockSize, progress, bufferSize),
         statistics);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    INode inode = checkFile(path);
    return new FSDataInputStream(new WTFInputStream(getConf(), store, inode,
                                                   statistics));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
      return false;
  }
  
  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  /**
   * FileStatus for WTF file systems. 
   */
  @Override
  public FileStatus getFileStatus(Path f)  throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(f));
    if (inode == null) {
      throw new FileNotFoundException(f + ": No such file or directory.");
    }
    return new WTFFileStatus(f.makeQualified(this), inode);
  }
}
