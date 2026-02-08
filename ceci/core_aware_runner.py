"""
This code has been generated with the help of an AI: Claude 4.5
"""

from . import minirunner
import subprocess
from timeit import default_timer
import sys
import heapq

# Job states (internal to this runner)
NOT_READY = 0
READY = 1
RUNNING = 2
COMPLETED = 3
FAILED = 4
BLOCKED = 5


class CoreAwareRunner(minirunner.Runner):
    """Optimized runner with state machine and lazy job creation.

    Extends the base Runner class to use compact state representation.
    Inherits run() method from parent, only overrides _update() and related methods.
    """

    def __init__(
        self,
        nodes,
        graph,
        job_maker,
        stage_priority,
        num_stages,
        log_dir,
        callback=None,
        sleep=None,
    ):
        """Initialize optimized runner.

        Parameters
        ----------
        nodes : list
            List of Node objects
        graph : dict
            Compact DAG representation with:
            - 'stage_dependencies': {stage_idx: [parent_indices]}
            - 'stage_children': {stage_idx: [child_indices]}
            - 'stage_metadata': {stage_name: metadata_dict} (from run_info[0])
            - 'iterator_generator': generator for lazy iterator discovery
        job_maker : JobMaker
            JobMaker for creating Job objects on-demand
        stage_priority : dict
            {stage_idx: priority} (lower = higher priority)
        num_stages : int
            Number of stages in pipeline
        log_dir : str
            Directory for log files
        callback : callable, optional
            Callback for events
        sleep : callable, optional
            Sleep function (for testing)
        """
        # Don't call super().__init__() - we have different initialization
        self.nodes = nodes
        self.log_dir = log_dir

        # Set callback and sleep following parent pattern
        if callback is None:
            callback = minirunner.null_callback
        if sleep is None:
            import time

            sleep = time.sleep
        self.callback = callback
        self.sleep = sleep

        # Extract graph components
        self.depend = graph["stage_dependencies"]
        self.stage_children = graph["stage_children"]
        self.jobs = graph["stage_metadata"]
        self.iterator_generator = graph["iterator_generator"]

        self.stage_priority = stage_priority
        self.num_stages = num_stages
        self.job_maker = job_maker

        # Build reverse lookup: stage_idx -> stage_name
        self.stage_idx_to_name = {}
        for stage_name, metadata in self.jobs.items():
            self.stage_idx_to_name[metadata["stage_idx"]] = stage_name

        # Track available cores per node
        self.cores_available = {node.id: node.cores for node in self.nodes}

        # State tracking (sparse - only for discovered iterators)
        self.iterator_states = {}

        # Ready jobs priority queue
        self.ready = []

        # Running jobs
        self.running = []

        # Completed/failed tracking
        self.completed_jobs = set()
        self.failed_count = 0
        self.blocked_count = 0
        self.failed_jobs = []

        # Iterator discovery state
        self.iterator_exhausted = False
        self.discovered_count = 0

        # Populate initial batch of jobs
        print("Starting pipeline execution...")
        self._refill_from_generator(batch_size=1000)
        print(f"Initial batch: {self.discovered_count} inputs discovered")
        sys.stdout.flush()

    def _refill_from_generator(self, batch_size=100):
        """Pull more iterator values from generator and add to ready queue."""

        if self.iterator_exhausted or self.iterator_generator is None:
            return

        count = 0
        for iter_val in self.iterator_generator:
            # Initialize state for this iterator value
            if iter_val not in self.iterator_states:
                states = [NOT_READY] * self.num_stages

                # Mark stages with no dependencies as READY
                for stage_idx in range(self.num_stages):
                    if not self.depend.get(stage_idx, []):
                        states[stage_idx] = READY

                self.iterator_states[iter_val] = states
                self.discovered_count += 1

                # Add ready jobs to queue
                for stage_idx in range(self.num_stages):
                    if states[stage_idx] == READY:
                        priority = self.stage_priority.get(stage_idx, 999)
                        heapq.heappush(
                            self.ready, (priority, stage_idx, iter_val)
                        )

            count += 1
            if count >= batch_size:
                return  # More items available, will continue later

        # If we reach here, the for loop completed naturally (generator exhausted)
        self.iterator_exhausted = True
        print(
            f"Iterator discovery complete: {self.discovered_count} total inputs"
        )
        sys.stdout.flush()

    def _update(self):
        """Check progress, launch jobs, and return status.

        This is the core scheduling logic, called repeatedly by run().

        Returns
        -------
        int
            minirunner.WAITING: still running
            minirunner.COMPLETE: all jobs done
        """
        # 1. Check for completed/failed jobs
        self._check_completed()

        # 2. Refill ready queue if getting low
        if len(self.ready) < 100 and not self.iterator_exhausted:
            self._refill_from_generator(batch_size=100)

        # 3. Try to launch ready jobs
        self._launch_ready_jobs()

        # 4. Check if pipeline is done
        if not self.ready and not self.running and self.iterator_exhausted:
            self._print_final_report()
            return minirunner.COMPLETE

        return minirunner.WAITING

    def _launch_ready_jobs(self):
        """Try to launch as many ready jobs as possible."""
        while self.ready:
            priority, stage_idx, iter_val = heapq.heappop(self.ready)

            # Verify still ready
            if self.iterator_states[iter_val][stage_idx] != READY:
                continue

            # Get stage metadata
            stage_name = self.stage_idx_to_name[stage_idx]
            metadata = self.jobs[stage_name]

            # Create Job object on-demand
            job = self.job_maker.create_job(metadata, iter_val)

            # Check resource availability (using job object, like original)
            alloc = self._check_availability(job)

            if alloc is not None:
                # Launch job (pass stage_idx and iter_val for tracking)
                self._launch(job, alloc, stage_idx, iter_val)
            else:
                # This job has the LOWEST resource requirements in ready queue
                # If it can't run, nothing else can either
                heapq.heappush(self.ready, (priority, stage_idx, iter_val))
                break

    def _check_availability(self, job):
        """Check if nodes with enough cores are available.

        Parameters
        ----------
        job : Job
            Job object to check resources for

        Returns
        -------
        list of Node or None
            List of allocated nodes, or None if unavailable
        """
        cores_needed = job.cores
        nodes_needed = job.nodes

        if nodes_needed == 1:
            # Single node job - can use partial node
            for node in self.nodes:
                if self.cores_available[node.id] >= cores_needed:
                    return [node]
            return None

        # Multi-node jobs need full nodes
        available_full_nodes = [
            node
            for node in self.nodes
            if self.cores_available[node.id] == node.cores
        ]

        if len(available_full_nodes) >= nodes_needed:
            return available_full_nodes[:nodes_needed]

        return None

    def _all_dependencies_met(self, stage_idx, iter_val):
        """Check if all dependencies for a stage are completed."""
        parent_stages = self.depend.get(stage_idx, [])
        states = self.iterator_states[iter_val]

        for parent_idx in parent_stages:
            if states[parent_idx] != COMPLETED:
                return False

        return True

    def _launch(self, job, alloc, stage_idx, iter_val):
        """Launch a job and track its core allocation.

        Parameters
        ----------
        job : Job
            Job object to launch
        alloc : list of Node
            Nodes allocated to this job
        stage_idx : int
            Stage index (for state tracking)
        iter_val : str
            Iterator value (for state tracking)
        """
        # Allocate cores
        cores_per_node = job.cores // len(alloc)
        for node in alloc:
            self.cores_available[node.id] -= cores_per_node
            if self.cores_available[node.id] == 0:
                node.assign()

        # Update state
        self.iterator_states[iter_val][stage_idx] = RUNNING

        # Launch process (following original pattern)
        print(f"\nExecuting {job.name}")
        print(f"  Cores: {job.cores} across {len(alloc)} node(s)")

        cmd = job.cmd
        print(f"Command is:\n{cmd}")
        stdout_file = f"{self.log_dir}/{job.name}.out"
        print(f"Output writing to {stdout_file}\n")

        with open(stdout_file, "w") as stdout:
            p = subprocess.Popen(
                cmd, shell=True, stdout=stdout, stderr=subprocess.STDOUT
            )

            # Store in running list - extended format with stage_idx and iter_val
            self.running.append(
                (p, job, alloc, default_timer(), stage_idx, iter_val)
            )

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
        """Check for completed jobs and update states."""
        completed = []
        continuing = []

        for entry in self.running:
            # Unpack extended format
            p, job, alloc, start_time, stage_idx, iter_val = entry
            status = p.poll()

            if status is None:
                # Still running
                continuing.append(entry)
                continue

            # Calculate cores per node for freeing resources
            cores_per_node = job.cores // len(alloc)

            if status != 0:
                # Job failed - mark as failed but continue pipeline (different from original!)
                run_time = minirunner.make_run_time_string(
                    default_timer() - start_time
                )
                print(
                    f"\n❌ Job {job.name} FAILED with status {status} after {run_time}"
                )
                print(f"   Log: {self.log_dir}/{job.name}.out")

                self.iterator_states[iter_val][stage_idx] = FAILED
                self.failed_count += 1
                self.failed_jobs.append(
                    {
                        "name": job.name,
                        "status": status,
                        "run_time": run_time,
                        "log": f"{self.log_dir}/{job.name}.out",
                    }
                )

                # Mark downstream jobs as blocked
                blocked_count = self._mark_blocked(stage_idx, iter_val)
                self.blocked_count += blocked_count

                self.callback(
                    minirunner.EVENT_FAIL,
                    {
                        "job": job,
                        "status": status,
                        "process": p,
                        "nodes": alloc,
                    },
                )

                # Free resources
                for node in alloc:
                    self.cores_available[node.id] += cores_per_node
                    node.free()

            else:
                # Job completed successfully
                run_time = minirunner.make_run_time_string(
                    default_timer() - start_time
                )
                print(
                    f"✓ Job {job.name} has completed successfully in {run_time}"
                )

                self.iterator_states[iter_val][stage_idx] = COMPLETED
                self.completed_jobs.add(job.name)
                completed.append((stage_idx, iter_val))

                # Free resources
                for node in alloc:
                    self.cores_available[node.id] += cores_per_node
                    node.free()
                    print(
                        f"  Freed {cores_per_node} cores on {node.id} "
                        f"({self.cores_available[node.id]}/{node.cores} available)"
                    )

                self.callback(
                    minirunner.EVENT_COMPLETED,
                    {
                        "job": job,
                        "status": 0,
                        "process": p,
                        "nodes": alloc,
                    },
                )

        self.running = continuing

        # Update dependencies for completed jobs
        for stage_idx, iter_val in completed:
            self._update_ready_jobs(stage_idx, iter_val)

        sys.stdout.flush()

    def _mark_blocked(self, failed_stage_idx, iter_val):
        """Mark all downstream jobs as blocked for this iterator value."""
        states = self.iterator_states[iter_val]
        blocked_count = 0

        # Recursively mark children as blocked
        def mark_children(stage_idx):
            nonlocal blocked_count
            for child_idx in self.stage_children.get(stage_idx, []):
                if states[child_idx] not in [
                    COMPLETED,
                    FAILED,
                    BLOCKED,
                    RUNNING,
                ]:
                    states[child_idx] = BLOCKED
                    blocked_count += 1
                    mark_children(child_idx)

        mark_children(failed_stage_idx)
        return blocked_count

    def _update_ready_jobs(self, completed_stage_idx, iter_val):
        """Update ready queue after a job completes."""
        # Check children of completed stage
        for child_idx in self.stage_children.get(completed_stage_idx, []):
            # Check if all dependencies are now met
            if self._all_dependencies_met(child_idx, iter_val):
                states = self.iterator_states[iter_val]
                if states[child_idx] == NOT_READY:
                    states[child_idx] = READY
                    priority = self.stage_priority.get(child_idx, 999)
                    heapq.heappush(self.ready, (priority, child_idx, iter_val))

    def _print_final_report(self):
        """Print final report after pipeline finishes."""
        total_processed = (
            len(self.completed_jobs) + self.failed_count + self.blocked_count
        )

        print(f"\n{'=' * 60}")
        print("Pipeline execution finished!")
        print(f"  Total inputs processed: {self.discovered_count}")
        print(f"  Total jobs: {total_processed}")
        print(f"  ✓ Completed: {len(self.completed_jobs)}")
        print(f"  ✗ Failed: {self.failed_count}")
        print(f"  ⊘ Blocked: {self.blocked_count}")

        if self.failed_jobs:
            print(f"\n{'=' * 60}")
            print(f"Failed jobs ({len(self.failed_jobs)}):")
            for failed in self.failed_jobs:
                print(
                    f"  • {failed['name']} (status {failed['status']}, {failed['run_time']})"
                )
                print(f"    Log: {failed['log']}")

        print(f"{'=' * 60}\n")

    def abort(self):
        """Abort all running jobs."""
        print("\n⚠️  Aborting all running jobs...")

        for entry in self.running:
            p, job, alloc, _, stage_idx, iter_val = entry
            print(f"  Killing {job.name}")
            p.kill()

            # Free resources
            cores_per_node = job.cores // len(alloc)
            for node in alloc:
                self.cores_available[node.id] += cores_per_node
                node.free()

        # Call callback with job list (matching original format)
        self.callback(
            minirunner.EVENT_ABORT,
            {"running": [(e[0], e[1], e[2], e[3]) for e in self.running]},
        )
        self.running = []
