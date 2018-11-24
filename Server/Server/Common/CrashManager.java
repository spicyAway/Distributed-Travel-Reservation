package Server.Common;

public class CrashManager{
  //This is the crash manager for participants
  public int mode;
  public CrashManager(){
    this.mode = -1;
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
    if(mode == 5)
      crash();
  }
}
