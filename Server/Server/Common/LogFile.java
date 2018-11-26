package Server.Common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

@SuppressWarnings("unchecked")
public class LogFile <T> extends DiskFile{
  public LogFile(String resourceManager, String fileName){
    this.filePath = dir + "/" + resourceManager + "/logfiles/" + fileName + ".txt";
    this.file = new File(filePath);
  }
  public void save(T data, String debug) throws IOException{
    super.save(data);
  }
}
