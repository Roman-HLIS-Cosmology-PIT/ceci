"""
This code has been generated with the help of an AI: Claude 4.5
"""

from .mini import MiniPipeline
from .. import core_aware_runner
from .iterator import Iterator
from .job_maker import JobMaker
import os
from pathlib import Path
import sys


class MultiInputMiniPipeline(MiniPipeline):
    """Pipeline that processes multiple input files using templated paths.

    This pipeline requires an iterator configuration to process multiple inputs.
    Uses lazy job creation - no job objects stored per iterator value.
    """

    def __init__(self, *args, pipe_config=None, **kwargs):
        """Initialize multi-input pipeline."""
        kwargs.pop("pipe_config", None)

        self.pipe_config = pipe_config or {}

        # Parse iterator configuration - REQUIRED
        iterator_config = self.pipe_config.get("iterator", None)

        if not iterator_config:
            raise ValueError(
                "MultiInputMiniPipeline requires an 'iterator' configuration. "
                "For single-input pipelines, use 'mini' launcher instead."
            )

        # Create iterator object
        self.iterator = Iterator(
            iterator_id=iterator_config["iterator_id"],
            pattern=iterator_config["pattern"],
        )

        # Parse output structure
        output_config = self.pipe_config.get(
            "output_structure", {"mode": "by_stage"}
        )
        self.output_mode = output_config.get("mode", "by_stage")

        # DON'T store iterator values - keep as generator
        self.iterator_generator = None

        super().__init__(*args, **kwargs)

    def _get_output_dir_for_stage(self, stage_name, iterator_value=None):
        """Get output directory for a stage/iteration."""
        base_output_dir = self.run_config["output_dir"]

        if self.output_mode == "flat":
            output_dir = base_output_dir
        elif self.output_mode == "by_stage":
            output_dir = os.path.join(base_output_dir, stage_name)
        elif self.output_mode == "by_stage_and_input":
            if iterator_value is not None:
                output_dir = os.path.join(
                    base_output_dir, stage_name, str(iterator_value)
                )
            else:
                output_dir = os.path.join(base_output_dir, stage_name)
        else:
            raise ValueError(f"Unknown output mode: {self.output_mode}")

        os.makedirs(output_dir, exist_ok=True)
        return output_dir

    def _make_output_template(self, base_path, output_dir):
        """Create output template path with iterator placeholder."""
        p = Path(base_path)
        template_filename = (
            f"{p.stem}{{{self.iterator.iterator_id}}}{p.suffix}"
        )
        return str(Path(output_dir) / template_filename)

    def _build_resource_priority_map(self):
        """Build priority map based on resource requirements.

        Returns
        -------
        dict
            {stage_idx: priority} where priority is based on core requirements
            (lower cores = lower priority number = scheduled first)
        """
        # Collect unique resource requirements per stage
        stage_resources = {}
        for stage_idx, stage in enumerate(self.stages):
            sec = self.stage_execution_config[stage.instance_name]
            cores = sec.threads_per_process * sec.nprocess
            stage_resources[stage_idx] = cores

        # Create priority mapping: sort by resources ascending
        unique_resources = sorted(set(stage_resources.values()))
        resource_to_priority = {
            cores: priority for priority, cores in enumerate(unique_resources)
        }

        # Map each stage to its priority
        stage_priority = {
            stage_idx: resource_to_priority[cores]
            for stage_idx, cores in stage_resources.items()
        }

        return stage_priority

    def enqueue_job(self, stage, pipeline_files):
        """Store stage metadata in run_info[0]. No job objects created.

        Jobs will be created on-demand by the runner when ready to execute.
        Stores metadata indexed by stage.instance_name (matches original pattern).
        """
        # Find the first input template that contains the iterator placeholder
        input_templates = self.pipe_config.get("inputs", {})
        marker = f"{{{self.iterator.iterator_id}}}"

        file_path = None
        for tag, template in input_templates.items():
            if marker in template:
                file_path = template
                break

        if file_path is None:
            raise ValueError(
                f"No input file template contains '{marker}'. "
                f"Available inputs: {list(input_templates.keys())}"
            )

        # Get stage index and execution config
        stage_idx = self.stages.index(stage)
        sec = self.stage_execution_config[stage.instance_name]

        # Get base outputs (templates)
        base_outputs = stage.find_outputs(self.run_config["output_dir"])

        # Get base output directory (without iterator suffix)
        output_dir = self._get_output_dir_for_stage(stage.instance_name)

        # Store stage-level metadata in run_info[0] using stage.instance_name as key
        # This matches the original pattern where keys are human-readable names
        self.run_info[0][stage.instance_name] = {
            "stage": stage,
            "stage_name": stage.instance_name,
            "stage_idx": stage_idx,
            "cores": sec.threads_per_process * sec.nprocess,
            "nodes": sec.nodes,
            "input_templates": input_templates,
            "pipeline_files": pipeline_files,
            "output_dir": output_dir,
            "stage_execution_config": sec,
            "base_outputs": base_outputs,
        }

        # Create generator (only once, for first stage)
        if self.iterator_generator is None:
            self.iterator_generator = self.iterator.generate_values(file_path)

        # Add stage to run_info[1]
        if stage not in self.run_info[1]:
            self.run_info[1].append(stage)

        # Build output templates
        templated_outputs = {}
        for tag in stage.output_tags():
            aliased_tag = stage.get_aliased_tag(tag)
            base_path = base_outputs[aliased_tag]
            templated_outputs[aliased_tag] = self._make_output_template(
                base_path, output_dir
            )

        return templated_outputs

    def build_stage_dag(self):
        """Build stage-level dependency graph.

        Returns stage dependencies using Stage objects (original convention).
        """
        output_to_stage = {}
        for stage in self.stages:
            for tag in stage.output_tags():
                aliased_tag = stage.get_aliased_tag(tag)
                output_to_stage[aliased_tag] = stage

        stage_dag = {}
        for stage in self.stages:
            parent_stages = []
            for tag in stage.input_tags():
                aliased_tag = stage.get_aliased_tag(tag)
                if aliased_tag in output_to_stage:
                    parent_stage = output_to_stage[aliased_tag]
                    if parent_stage not in parent_stages:
                        parent_stages.append(parent_stage)
            stage_dag[stage] = parent_stages

        return stage_dag

    def build_dag(self, jobs):
        """Build compact DAG representation with stage dependencies.

        Parameters
        ----------
        jobs : dict
            run_info[0] containing stage metadata indexed by stage.instance_name

        Returns
        -------
        dict
            Compact representation with stage dependencies and iterator generator
        """
        # Build stage-level DAG
        stage_dag = self.build_stage_dag()

        # Convert Stage objects to indices
        stage_to_idx = {stage: idx for idx, stage in enumerate(self.stages)}

        # Build dependency maps using indices
        stage_dependencies = {}
        stage_children = {i: [] for i in range(len(self.stages))}

        for stage, parent_stages in stage_dag.items():
            stage_idx = stage_to_idx[stage]
            parent_indices = [stage_to_idx[ps] for ps in parent_stages]
            stage_dependencies[stage_idx] = parent_indices

            # Build reverse map (children)
            for parent_idx in parent_indices:
                stage_children[parent_idx].append(stage_idx)

        # Return compact representation with generator
        return {
            "stage_dependencies": stage_dependencies,
            "stage_children": stage_children,
            "stage_metadata": jobs,
            "iterator_generator": self.iterator_generator,
        }

    def run_jobs(self):
        """Run with optimized core-aware scheduling."""

        jobs = self.run_info[0]  # Stage metadata dict

        print(f"\n{'=' * 60}")
        print("Running multi-input pipeline")
        print(f"Iterator: {self.iterator.iterator_id}")
        print(f"Number of stages: {len(self.stages)}")
        print(f"Output mode: {self.output_mode}")
        print("Discovering inputs lazily...")
        print(f"{'=' * 60}\n")

        # Build DAG structures
        graph = self.build_dag(jobs)

        # Build priority map
        stage_priority = self._build_resource_priority_map()

        # Create job maker
        job_maker = JobMaker(
            self.iterator, self.stages_config, self.run_config
        )

        # Get execution config
        sec = self.stage_execution_config[self.stage_names[0]]
        nodes = sec.site.info["nodes"]
        log_dir = self.run_config["log_dir"]

        # Create optimized runner
        runner = core_aware_runner.CoreAwareRunner(
            nodes=nodes,
            graph=graph,
            job_maker=job_maker,
            stage_priority=stage_priority,
            num_stages=len(self.stages),
            log_dir=log_dir,
            callback=self.callback,
            sleep=self.sleep,
        )

        interval = self.launcher_config.get("interval", 3)
        status = runner.run(interval)

        # Check for failures
        if runner.failed_count > 0:
            sys.stderr.write(
                f"""
    *************************************************
    Pipeline completed with failures.
    See logs in {log_dir} for details.
    *************************************************
    """
            )
            return 1

        return 0

    def find_all_outputs(self):
        """Find all outputs as templates."""
        templated_outputs = {}

        for stage in self.stages:
            output_dir = self._get_output_dir_for_stage(stage.instance_name)

            for tag in stage.output_tags():
                aliased_tag = stage.get_aliased_tag(tag)
                base_outputs = stage.find_outputs(
                    self.run_config["output_dir"]
                )
                base_path = base_outputs[aliased_tag]
                templated_outputs[aliased_tag] = self._make_output_template(
                    base_path, output_dir
                )

        return templated_outputs
