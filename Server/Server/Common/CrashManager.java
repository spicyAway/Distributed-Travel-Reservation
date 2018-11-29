package Server.Common;
import java.io.IOException;

@SuppressWarnings("unchecked")
public class CrashManager{
  //This is the crash manager for participants
  public int mode;
  public static LogFile<Integer> log_mode;
  public CrashManager(String rm_name){
    this.mode = -1;
    this.log_mode = new LogFile(rm_name, "Logged-Crash-Mode");
    load();
  }
  public void load(){
    try{
      this.mode = (int) log_mode.read();
      //System.out.print("**DEBUG LOAD MODE: " + mode + "\n");
    }catch(Exception e){
      this.save();
    }
  }

  public void save(){
    try{
      log_mode.save(mode, "debug");
    }catch(IOException e){
      e.printStackTrace();
      System.out.print("Crash Manager save file failed." + "\n");
    }
  }
  public void crash(){
    System.out.print("[[[Ready to crash]]]" + "\n");
    System.exit(1);
  }
  public void resetCrashes(){
    this.mode = -1;
  }
  public void rec_req_before_send(){
    if(mode == 1)
      crash();
  }
  public void after_decision(){
    if(mode == 2)
      crash();
  }
  public void after_sending(){
    if(mode == 3)
      crash();
  }
  public void after_rec_before_operate(){
    if(mode == 4)
      crash();
  }
  public void during_recovery(){
    if(mode == 5){
    //Avoid re-crash
      mode = -1;
      this.save();
      crash();
    }
  }
}
