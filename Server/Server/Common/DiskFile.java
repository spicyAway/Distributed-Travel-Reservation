package Server.Common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

@SuppressWarnings("unchecked")
public class DiskFile <T> implements Serializable{

   public static final String dir = "data";
   protected File file;
   public String filePath;

   public DiskFile(){
     //dummy
   }
   public DiskFile(String resourceManager, String fileName){
     this.filePath = dir + "/" + resourceManager + "/datafiles/" + fileName + ".txt";
     this.file = new File(filePath);
   }

   public void save(T data) throws IOException{
     //If file exists do nohting; if not, create a new file
     this.file.getParentFile().mkdirs();
     this.file.createNewFile();
     FileOutputStream fos = new FileOutputStream(this.file);
     ObjectOutputStream oos = new ObjectOutputStream(fos);
     oos.writeObject(data);
     oos.close();
     fos.close();
   }

   public T read() throws IOException, ClassNotFoundException{
     FileInputStream fis = new FileInputStream(this.file);
     ObjectInputStream ois = new ObjectInputStream(fis);
     T data = (T) ois.readObject();
     ois.close();
     fis.close();
     return data;
   }

}
