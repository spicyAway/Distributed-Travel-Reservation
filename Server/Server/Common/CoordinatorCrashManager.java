package Server.Common;

public class CoordinatorCrashManager{
  //This is the crash manager for participants
  public int mode;
  public CoordinatorCrashManager(){
    this.mode = -1;
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
    if(mode == 8)
      crash();
  }
}
