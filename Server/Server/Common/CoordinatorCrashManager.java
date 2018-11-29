package Server.Common;
import java.io.IOException;
@SuppressWarnings("unchecked")
public class CoordinatorCrashManager{
  //This is the crash manager for participants
  public int mode;
  public static LogFile<Integer> log_mode;
  public CoordinatorCrashManager(String mw_name){
    this.mode = -1;
    this.log_mode = new LogFile(mw_name, "Logged-Crash-Mode");
    load();
  }

  public void load(){
    try{
      this.mode = (int) log_mode.read();
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
    System.out.print("[[[Coordinator Ready to crash]]]" + "\n");
    System.exit(1);
  }
  public void resetCrashes(){
    this.mode = -1;
  }
  public void before_vote(){
    if(mode == 1)
      crash();
  }
  public void after_vote_before_rec(){
    if(mode == 2)
      crash();
  }
  public void rec_some_rep(){
    if(mode == 3)
      crash();
  }
  public void rec_all_before_dec(){
    if(mode == 4)
      crash();
  }
  public void after_dec_before_send(){
    if(mode == 5)
      crash();
  }
  public void send_some_dec(){
    if(mode == 6)
      crash();
  }
  public void after_send_all_dec(){
    if(mode == 7)
      crash();
  }
  public void during_recovery(){
    if(mode == 8){
    //Avoid re-crash
      mode = -1;
      this.save();
      crash();
    }
  }
}
