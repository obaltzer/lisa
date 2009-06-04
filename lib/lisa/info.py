import os
try:
    from threading import current_thread
except:
    from threading import currentThread as current_thread

class ThreadInfo(object):
    def __init__(self):
        self._info = {
            'name': current_thread().getName(),
            'pid': os.getpid(),
        }
        try:
            # Please do not crash!!!
            import ctypes
            libc = ctypes.CDLL('libc.so.6')
            # try 64bit syscall for gettid first
            tid = libc.syscall(186)
            if tid < 0:
                # if failed try 32bit
                tid = libc.syscall(224)

            pid = os.getpid()

            self._info['tid'] = tid
            
            # see sched_debug.c for details
            f = open('/proc/%d/task/%d/sched' % (pid, tid), 'r')
            for l in f:
                if l.startswith('se.sum_exec_runtime'):
                    self._info['time'] = float(l.split(':')[1].strip())
                elif l.startswith('nr_voluntary_switches'):
                    self._info['nr_vsw'] = int(l.split(':')[1].strip())
                elif l.startswith('nr_involuntary_switches'):
                    self._info['nr_ivsw'] = int(l.split(':')[1].strip())
            f.close()
        except Exception, e:
            pass
    
    def __repr__(self):
        return '[%-20s: %8d] time: %10.2f , vsw: %8d , ivsw: %8d' % (
            self._info['name'], 
            self._info['tid'],
            self._info['time'],
            self._info['nr_vsw'],
            self._info['nr_ivsw']
        )
