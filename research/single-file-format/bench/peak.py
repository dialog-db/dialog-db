"""Runs a command; prints its wall time, peak anonymous memory (heap, not
the file's mapped pages, which RSS counts once per mapping), and page faults."""
import resource, subprocess, sys, time
started = time.time()
child = subprocess.Popen(sys.argv[1:])
peak = 0
while child.poll() is None:
    try:
        for line in open(f"/proc/{child.pid}/status"):
            if line.startswith("RssAnon:"):
                peak = max(peak, int(line.split()[1]))
    except OSError:
        pass
    time.sleep(0.1)
usage = resource.getrusage(resource.RUSAGE_CHILDREN)
print(f"exit {child.returncode}; wall {time.time() - started:.1f} s; peak anonymous {peak / 1024:.1f} MiB; "
      f"minor faults {usage.ru_minflt}; major faults {usage.ru_majflt}", flush=True)
