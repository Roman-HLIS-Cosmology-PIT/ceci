"""Core-aware runner for multi-input pipelines."""

from . import minirunner
from timeit import default_timer
import sys


class CoreAwareRunner(minirunner.Runner):
    """Runner that tracks core usage for fine-grained resource allocation.

    The base Runner treats nodes as all-or-nothing (assigned/not assigned).
    CoreAwareRunner tracks available cores per node and only allocates the
    cores actually needed by each job, allowing multiple jobs to share a node.
    """

    def __init__(
        self, nodes, job_graph, log_dir, stage_priorities=None, **kwargs
    ):
        super().__init__(nodes, job_graph, log_dir, **kwargs)

        # No need to check/convert completed_jobs - parent uses set now!

        # Track available cores per node
        self.cores_available = {node.id: node.cores for node in self.nodes}

        # Stage priorities for scheduling
        self.stage_priorities = stage_priorities or {}

        # Track core allocations for running jobs
        self.job_allocations = {}

    def _check_availability(self, job):
        """Check if nodes with enough cores are available."""
        if job.nodes == 1:
            for node in self.nodes:
                if self.cores_available[node.id] >= job.cores:
                    return [node]
            return None

        # Multi-node jobs need full nodes
        available_full_nodes = [
            node
            for node in self.nodes
            if self.cores_available[node.id] == node.cores
        ]

        if len(available_full_nodes) >= job.nodes:
            return available_full_nodes[: job.nodes]

        return None

    def _ready_jobs(self):
        """Get jobs ready to run, sorted by priority."""
        ready = super()._ready_jobs()

        if not self.stage_priorities:
            return ready

        def get_priority(job):
            stage_name = (
                job.name.split("[")[0] if "[" in job.name else job.name
            )
            return self.stage_priorities.get(stage_name, 999)

        ready.sort(key=get_priority)
        return ready

    def _launch(self, job, alloc):
        """Launch a job and track its core allocation."""
        print(f"\nExecuting {job.name}")

        cores_per_node = job.cores // len(alloc)
        allocation = {}

        for node in alloc:
            self.cores_available[node.id] -= cores_per_node
            allocation[node] = cores_per_node

            if self.cores_available[node.id] == 0:
                node.assign()

            print(
                f"  Allocated {cores_per_node} cores on {node.id} "
                f"({self.cores_available[node.id]}/{node.cores} remaining)"
            )

        self.job_allocations[job] = allocation

        cmd = job.cmd
        print(f"Command is:\n{cmd}")
        stdout_file = f"{self.log_dir}/{job.name}.out"
        print(f"Output writing to {stdout_file}\n")

        import subprocess

        with open(stdout_file, "w") as stdout:
            p = subprocess.Popen(
                cmd, shell=True, stdout=stdout, stderr=subprocess.STDOUT
            )
            self.running.append((p, job, alloc, default_timer()))
            self.callback(
                minirunner.EVENT_LAUNCH,
                {
                    "job": job,
                    "stdout": stdout_file,
                    "process": p,
                    "nodes": alloc,
                },
            )

        sys.stdout.flush()

    def _check_completed(self):
        """Check for completed jobs and free their cores."""
        completed_jobs = []
        continuing_jobs = []

        for process, job, alloc, start_time in self.running:
            status = process.poll()

            if status is None:
                continuing_jobs.append((process, job, alloc, start_time))

            elif status:
                print(f"Job {job.name} has failed with status {status}")
                self.callback(
                    minirunner.EVENT_FAIL,
                    {
                        "job": job,
                        "status": status,
                        "process": process,
                        "nodes": alloc,
                    },
                )
                fail_time = minirunner.make_run_time_string(
                    default_timer() - start_time
                )
                self.abort()
                raise minirunner.FailedJob(job.cmd, job.name, fail_time)

            else:
                run_time = minirunner.make_run_time_string(
                    default_timer() - start_time
                )
                print(
                    f"Job {job.name} has completed successfully in {run_time}"
                )
                self.callback(
                    minirunner.EVENT_COMPLETED,
                    {
                        "job": job,
                        "status": 0,
                        "process": process,
                        "nodes": alloc,
                    },
                )
                completed_jobs.append(job)

                # Free cores
                allocation = self.job_allocations[job]
                for node, cores_allocated in allocation.items():
                    self.cores_available[node.id] += cores_allocated
                    node.free()
                    print(
                        f"  Freed {cores_allocated} cores on {node.id} "
                        f"({self.cores_available[node.id]}/{node.cores} available)"
                    )

                del self.job_allocations[job]

        sys.stdout.flush()
        self.running = continuing_jobs

        for job in completed_jobs:
            self.completed_jobs.add(job)  # Works with set (from parent)

    def abort(self):
        """Abort all running jobs and reset core tracking."""
        for process, job, alloc, _ in self.running:
            process.kill()

        for node in self.nodes:
            self.cores_available[node.id] = node.cores
            node.free()

        self.job_allocations = {}

        self.callback(minirunner.EVENT_ABORT, {"running": list(self.running)})
        self.running = []
