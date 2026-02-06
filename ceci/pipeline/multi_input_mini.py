from .mini import MiniPipeline
from .. import minirunner
from .. import core_aware_runner
from .iterator import Iterator
import os
from pathlib import Path
import sys


class MultiInputMiniPipeline(MiniPipeline):
    """Pipeline that processes multiple input files using templated paths.

    This pipeline requires an iterator configuration to process multiple inputs.
    For single-input pipelines, use MiniPipeline instead.
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

        # Create iterator object with just id and pattern
        self.iterator = Iterator(
            iterator_id=iterator_config["iterator_id"],
            pattern=iterator_config["pattern"],
        )

        # Parse output structure
        output_config = self.pipe_config.get(
            "output_structure", {"mode": "by_stage"}
        )
        self.output_mode = output_config.get("mode", "by_stage")

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

    def _make_output_path(self, base_path, output_dir, suffix):
        """Create output path with proper directory and suffix."""
        p = Path(base_path)
        new_filename = f"{p.stem}{suffix}{p.suffix}"
        return str(Path(output_dir) / new_filename)

    def _make_output_template(self, base_path, output_dir):
        """Create output template path with iterator placeholder.

        Parameters
        ----------
        base_path : str
            Base output path from stage.find_outputs()
        output_dir : str
            Directory for this stage

        Returns
        -------
        str
            Template path with {iterator_id} placeholder
        """
        p = Path(base_path)
        template_filename = (
            f"{p.stem}{{{self.iterator.iterator_id}}}{p.suffix}"
        )
        return str(Path(output_dir) / template_filename)

    def enqueue_job(self, stage, pipeline_files):
        """Create multiple jobs using template-based paths."""

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

        # Generate values and create jobs on-demand
        for iterator_value in self.iterator.generate_values(file_path):
            # Create job name using iterator
            job_name = self.iterator.make_job_name(
                stage.instance_name, iterator_value
            )

            # Build input files for this job
            files_for_this_run = {}

            # Expand templates from pipeline_files (previous stage outputs)
            for tag, path_template in pipeline_files.items():
                files_for_this_run[tag] = self.iterator.expand_template(
                    path_template, iterator_value
                )

            # Add inputs for this iteration
            for tag, template in input_templates.items():
                path = self.iterator.expand_template(template, iterator_value)
                files_for_this_run[tag] = path

            # Generate outputs for this iteration
            sec = self.stage_execution_config[stage.instance_name]
            output_dir = self._get_output_dir_for_stage(
                stage.instance_name, iterator_value
            )

            outputs = {}
            for tag in stage.output_tags():
                aliased_tag = stage.get_aliased_tag(tag)
                base_outputs = stage.find_outputs(
                    self.run_config["output_dir"]
                )
                base_path = base_outputs[aliased_tag]
                output_path = self._make_output_path(
                    base_path, output_dir, iterator_value
                )
                outputs[aliased_tag] = output_path

            # Generate command
            cmd = sec.generate_full_command(
                files_for_this_run, outputs, self.stages_config
            )

            # Create Job object
            job = minirunner.Job(
                job_name,
                cmd,
                cores=sec.threads_per_process * sec.nprocess,
                nodes=sec.nodes,
            )

            # Store job in run_info
            self.run_info[0][job_name] = job

        # Add stage to list
        if stage not in self.run_info[1]:
            self.run_info[1].append(stage)

        # Build output templates directly
        templated_outputs = {}
        output_dir = self._get_output_dir_for_stage(stage.instance_name)

        for tag in stage.output_tags():
            aliased_tag = stage.get_aliased_tag(tag)
            base_outputs = stage.find_outputs(self.run_config["output_dir"])
            base_path = base_outputs[aliased_tag]
            templated_outputs[aliased_tag] = self._make_output_template(
                base_path, output_dir
            )

        return templated_outputs

    def build_stage_dag(self):
        """Build stage-level dependency graph."""
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
        """Build DAG by replicating stage dependencies across files."""
        stage_dag = self.build_stage_dag()

        job_index = {}
        for job_name, job in jobs.items():
            stage_name, iterator_value = self.iterator.parse_job_name(job_name)

            for stage in self.stages:
                if stage.instance_name == stage_name:
                    job_index[(stage, iterator_value)] = job
                    break

        depend = {}

        # Get all unique iterator values from jobs
        iterator_values = set()
        for job_name in jobs.keys():
            _, iterator_value = self.iterator.parse_job_name(job_name)
            if iterator_value is not None:
                iterator_values.add(iterator_value)
        iterator_values = sorted(iterator_values)

        for iterator_value in iterator_values:
            for stage in self.stages:
                key = (stage, iterator_value)
                if key not in job_index:
                    continue

                job = job_index[key]
                parent_stages = stage_dag[stage]
                parent_jobs = [
                    job_index[(ps, iterator_value)]
                    for ps in parent_stages
                    if (ps, iterator_value) in job_index
                ]
                depend[job] = parent_jobs

        return depend

    def run_jobs(self):
        """Run with core-aware priority scheduling."""
        jobs, _ = self.run_info

        print(f"\n{'=' * 60}")
        print("Running multi-input pipeline")
        print(f"Iterator: {self.iterator.iterator_id}")
        print(f"Number of inputs: {len(jobs) // len(self.stages)}")
        print(f"Number of stages: {len(self.stages)}")
        print(f"Total jobs: {len(jobs)}")
        print(f"Output mode: {self.output_mode}")
        print(f"{'=' * 60}\n")

        graph = self.build_dag(jobs)

        sec = self.stage_execution_config[self.stage_names[0]]
        nodes = sec.site.info["nodes"]
        log_dir = self.run_config["log_dir"]

        stage_priorities = {
            stage.instance_name: i for i, stage in enumerate(self.stages)
        }

        runner = core_aware_runner.CoreAwareRunner(
            nodes,
            graph,
            log_dir,
            stage_priorities=stage_priorities,
            callback=self.callback,
            sleep=self.sleep,
        )

        interval = self.launcher_config.get("interval", 3)
        try:
            runner.run(interval)
        except minirunner.FailedJob as error:
            sys.stderr.write(
                f"""
*************************************************
Error running pipeline stage {error.job_name}.
Failed after {error.run_time}.

Standard output and error streams in {log_dir}/{error.job_name}.out
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
