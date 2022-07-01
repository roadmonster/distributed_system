import sys
import threading

"""
This class defines a way to kill a thread
"""
class KThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self, *args, *kwargs)
        self.killed = False
    
    def start(self):
        """
        Override the start()
        assign the original run() to run_backup()
        assign personalized _run() function to be run by the thread
        """
        self.__run_backup = self.run
        self.run = self._run
        threading.Thread.start(self)
    
    def __run(self):
        """
        have specialized run() which installs the trace into the tread
        """
        sys.settrace(self.globaltrace)
        self.__run_backup()
        self.run = self.__run_backup()
    
    def globaltrace(self, frame, event, arg):
        """
        func to set up global trace
        """
        if event == 'call':
            # if the new thread is called, event shall be call, 
            # hence return a local trace for this thread
            return self.localtrace
        else:
            return None

    def localtrace(self, frame, event, arg):
        # in this local thread, the flag is marked as done, then 
        if self.killed:
            # if current thread is going to exec a new line or loop back to exec
            if event == 'line':
                raise SystemExit()
        return self.localtrace
    
    def kill(self):
        self.killed = True
        

