"""Machine-local controller stdout/stderr and exit receipts; never run artifacts."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import sys
import threading

from maestro.storage.paths import run_paths


class Tee:
    def __init__(self,stream,log):
        self.stream,self.log=stream,log
        self.lock=threading.RLock()
    def write(self,text):
        with self.lock:
            self.log.write(text);self.log.flush()
            return self.stream.write(text)
    def flush(self):
        with self.lock:self.log.flush();self.stream.flush()
    def __getattr__(self,key):return getattr(self.stream,key)


@contextmanager
def controller_logs(root,command):
    local=run_paths(root).local;local.mkdir(parents=True,exist_ok=True,mode=0o700)
    original_out,original_err=sys.stdout,sys.stderr
    receipt={'command':command,'started_utc':datetime.now(timezone.utc).isoformat(),'exit_code':None}
    with (local/'controller.stdout.log').open('a') as out,(local/'controller.stderr.log').open('a') as err:
        try:
            sys.stdout,sys.stderr=Tee(original_out,out),Tee(original_err,err)
            yield receipt
        except BaseException as error:
            receipt['exit_code']=(130 if isinstance(error,KeyboardInterrupt) else
                error.code if isinstance(error,SystemExit) and isinstance(error.code,int) else 1)
            receipt['exception_class']=type(error).__name__
            raise
        finally:
            sys.stdout,sys.stderr=original_out,original_err
            receipt['finished_utc']=datetime.now(timezone.utc).isoformat()
            with (local/'controller-exits.jsonl').open('a') as file:
                file.write(json.dumps(receipt,sort_keys=True)+'\n')
