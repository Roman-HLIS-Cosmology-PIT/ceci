from .mini import MiniPipeline
from .. import minirunner
from .. import core_aware_runner
import os
from pathlib import Path
import collections
import sys


class MultiInputMiniPipeline(MiniPipeline):
    """Pipeline that processes multiple input files using templated paths.

    This maintains backward compatibility by storing templated paths in
    pipeline_file and expanding them on-the-fly when creating individual jobs.
    """

    def __init__(self, *args, pipe_config=None, **kwargs):
        """Initialize multi-input pipeline."""
        kwargs.pop("pipe_config", None)

        self.pipe_config = pipe_config or {}

        # Parse iterator configuration
        iterator_config = self.pipe_config.get("iterator", None)

        if iterator_config:
            # Get iterator configuration
            self.iterator_name = iterator_config["iterator_id"]
            iterator_pattern = iterator_config["pattern"]

            # Get input templates from the inputs section
            input_templates = self.pipe_config.get("inputs", {})

            # Discover iterator values by scanning the input files
            self.iterator_values = self._discover_iterator_values(
                input_templates, self.iterator_name, iterator_pattern
            )

            # Generate input file sets by substituting iterator values
            self.input_file_sets = self._generate_input_sets(
                input_templates, self.iterator_name, self.iterator_values
            )
        else:
            # No iterator - single execution (backward compatible)
            self.iterator_name = None
            self.iterator_values = [None]
            self.input_file_sets = [self.pipe_config.get("inputs", {})]

        # Parse output structure
        output_config = self.pipe_config.get(
            "output_structure", {"mode": "by_stage"}
        )
        self.output_mode = output_config.get("mode", "by_stage")

        super().__init__(*args, **kwargs)

    def _discover_iterator_values(self, input_templates, iterator_id, pattern):
        """Discover iterator values by scanning input files.

        Parameters
        ----------
        input_templates : dict
            Input file path templates with {iterator_id} placeholders
        iterator_id : str
            Name of iterator variable
        pattern : str
            Regex pattern to extract iterator values

        Returns
        -------
        list
            Sorted list of discovered iterator values
        """
        from .iterator import iter_matching_files

        # We need to convert input templates to the format expected by iter_matching_files
        # Take the first input template that contains the iterator placeholder
        marker = f"{{{iterator_id}}}"

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

        # Build config for iter_matching_files
        iter_config = {
            "iterator_id": iterator_id,
            "pattern": pattern,
            "file_path": file_path,
        }

        # Collect all matching files
        matches = list(iter_matching_files(iter_config))

        if not matches:
            raise ValueError(
                f"No files found matching path pattern: {file_path} "
                f"with pattern: {pattern}"
            )

        # Extract iterator values
        values = [match["iterator"] for match in matches]

        # Sort values
        values.sort()

        print(f"Iterator '{iterator_id}' found {len(values)} files")
        print(f"  Pattern: {pattern}")
        print(f"  Template: {file_path}")
        print(f"  Values: {values[:5]}{'...' if len(values) > 5 else ''}")

        return values

    def _generate_input_sets(
        self, input_templates, iterator_name, iterator_values
    ):
        """Generate input file sets by substituting iterator values.

        Parameters
        ----------
        input_templates : dict
            File path templates with {iterator_name} placeholders
        iterator_name : str
            Name of iterator variable
        iterator_values : list
            Values to substitute

        Returns
        -------
        list of dict
            One dict per iterator value with substituted paths
        """
        input_sets = []

        for value in iterator_values:
            input_set = {}
            for tag, path_template in input_templates.items():
                # Substitute {iterator_name} with actual value
                path = path_template.format(**{iterator_name: value})
                input_set[tag] = path
            input_sets.append(input_set)

        return input_sets

    def _make_template_path(self, concrete_paths):
        """Convert a list of concrete paths back to a template.

        Parameters
        ----------
        concrete_paths : list
            List of concrete paths like
        Returns
        -------
        str
            Template path
        """
        if not concrete_paths or not self.iterator_name:
            return concrete_paths[0] if concrete_paths else None

        # Take the first path and replace its iterator value with the template placeholder
        first_path = concrete_paths[0]
        first_value = str(self.iterator_values[0])

        # Replace the first iterator value with the template
        template_placeholder = f"{{{self.iterator_name}}}"
        template_path = first_path.replace(first_value, template_placeholder)

        # Verify this template works for all paths
        for i, path in enumerate(concrete_paths):
            expected = template_path.replace(
                template_placeholder, str(self.iterator_values[i])
            )
            if expected != path:
                # Fallback: if we can't create a simple template, return the first path
                return first_path

        return template_path

    def _expand_template(self, path_or_template, iteration_index):
        """Expand a template path with the specific iterator value.

        This is the KEY function that resolves cross-stage dependencies!

        Parameters
        ----------
        path_or_template : str
            Either a concrete path or a template with {iterator_name}
        iteration_index : int
            Which iteration we're on

        Returns
        -------
        str
            Concrete path with template substituted
        """
        if not self.iterator_name:
            return path_or_template

        # Check if it's a template
        template_pattern = f"{{{self.iterator_name}}}"
        if template_pattern in path_or_template:
            # Substitute with the specific value
            iterator_value = self.iterator_values[iteration_index]
            return path_or_template.replace(
                template_pattern, str(iterator_value)
            )

        return path_or_template

    def _get_output_dir_for_stage(self, stage_name, iteration_index=None):
        """Get output directory for a stage/iteration.

        Parameters
        ----------
        stage_name : str
            Name of the stage
        iteration_index : int, optional
            Index into iterator_values

        Returns
        -------
        str
            Output directory path
        """
        base_output_dir = self.run_config["output_dir"]

        if self.output_mode == "flat":
            output_dir = base_output_dir
        elif self.output_mode == "by_stage":
            output_dir = os.path.join(base_output_dir, stage_name)
        elif self.output_mode == "by_stage_and_input":
            if iteration_index is not None and self.iterator_name:
                iterator_value = self.iterator_values[iteration_index]
                output_dir = os.path.join(
                    base_output_dir, stage_name, str(iterator_value)
                )
            else:
                output_dir = os.path.join(base_output_dir, stage_name)
        else:
            raise ValueError(f"Unknown output mode: {self.output_mode}")

        os.makedirs(output_dir, exist_ok=True)
        return output_dir

    def _get_iterator_suffix(self, iteration_index):
        """Get suffix for output filenames.

        Parameters
        ----------
        iteration_index : int
            Index into iterator_values

        Returns
        -------
        str
            Suffix string
        """
        if self.iterator_name:
            iterator_value = self.iterator_values[iteration_index]
            return str(iterator_value)
        else:
            return ""

    def _make_output_path(self, base_path, output_dir, suffix):
        """Create output path with proper directory and suffix.

        Parameters
        ----------
        base_path : str
            Base output path from stage.find_outputs()
        output_dir : str
            Directory for this stage/iteration
        suffix : str
            Suffix to add to filename

        Returns
        -------
        str
            Modified output path
        """
        p = Path(base_path)
        new_filename = f"{p.stem}{suffix}{p.suffix}"
        return str(Path(output_dir) / new_filename)

    def enqueue_job(self, stage, pipeline_files):
        """Create multiple jobs using template-based paths.

        Cross-stage dependencies are resolved through template expansion:
        - pipeline_files contains templates like "output/{number}.txt"
        - For each iteration i, we expand templates using iterator_values[i]
        - This gives the concrete path that stage[i] produced
        - No separate tracking dict needed!

        Parameters
        ----------
        stage : PipelineStage
            The stage to create jobs for
        pipeline_files : dict
            Available files from previous stages as templates

        Returns
        -------
        dict
            Maps each output tag to a templated path
        """
        # Collect concrete outputs for template generation
        outputs_by_tag_lists = collections.defaultdict(list)

        for i, input_set in enumerate(self.input_file_sets):
            job_name = f"{stage.instance_name}[input_{i}]"

            # Build input files for this job by expanding ALL templates
            files_for_this_run = {}

            # Expand templates from pipeline_files for THIS iteration
            # This resolves cross-stage dependencies!
            for tag, path_template in pipeline_files.items():
                files_for_this_run[tag] = self._expand_template(
                    path_template, i
                )

            # Override with iteration-specific inputs (already concrete)
            files_for_this_run.update(input_set)

            # Generate outputs for this iteration
            sec = self.stage_execution_config[stage.instance_name]
            output_dir = self._get_output_dir_for_stage(stage.instance_name, i)
            suffix = self._get_iterator_suffix(i)

            outputs = {}
            for tag in stage.output_tags():
                aliased_tag = stage.get_aliased_tag(tag)
                base_outputs = stage.find_outputs(
                    self.run_config["output_dir"]
                )
                base_path = base_outputs[aliased_tag]
                output_path = self._make_output_path(
                    base_path, output_dir, suffix
                )
                outputs[aliased_tag] = output_path

                # Collect for template generation
                outputs_by_tag_lists[aliased_tag].append(output_path)

            # Generate command
            # generate_full_command() will internally call stage.find_inputs(files_for_this_run)
            # files_for_this_run already has the correct expanded paths!
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

        # Add stage to list (for backward compatibility)
        if stage not in self.run_info[1]:
            self.run_info[1].append(stage)

        # Convert concrete paths back to templates
        templated_outputs = {}
        for tag, paths in outputs_by_tag_lists.items():
            templated_outputs[tag] = self._make_template_path(paths)

        return templated_outputs

    def build_stage_dag(self):
        """Build stage-level dependency graph."""
        # Build reverse index: output_tag -> stage
        output_to_stage = {}
        for stage in self.stages:
            for tag in stage.output_tags():
                aliased_tag = stage.get_aliased_tag(tag)
                output_to_stage[aliased_tag] = stage

        # Build stage dependencies
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

        # Build stage-level DAG once
        stage_dag = self.build_stage_dag()

        # Build job index: (stage, file_index) -> Job
        job_index = {}
        for job_name, job in jobs.items():
            if "[input_" not in job_name:
                continue
            stage_name = job_name.split("[input_")[0]
            i = int(job_name.split("[input_")[1].rstrip("]"))
            for stage in self.stages:
                if stage.instance_name == stage_name:
                    job_index[(stage, i)] = job
                    break

        # Replicate dependencies across all files
        depend = {}
        for i in range(len(self.input_file_sets)):
            for stage in self.stages:
                key = (stage, i)
                if key not in job_index:
                    continue

                job = job_index[key]
                parent_stages = stage_dag[stage]
                parent_jobs = [
                    job_index[(ps, i)]
                    for ps in parent_stages
                    if (ps, i) in job_index
                ]
                depend[job] = parent_jobs

        return depend

    def run_jobs(self):
        """Run with core-aware priority scheduling."""
        jobs, _ = self.run_info

        print(f"\n{'=' * 60}")
        print(f"Running multi-input pipeline")
        print(f"Iterator: {self.iterator_name}")
        print(f"Number of inputs: {len(self.iterator_values)}")
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
        """Find all outputs as templates.

        Reconstructs outputs from the jobs that were created.

        Returns
        -------
        dict
            Maps tag -> templated path:
            {"tag": "output/file{number}.txt"}
        """
        outputs_by_tag = collections.defaultdict(list)

        # Extract outputs from all jobs that were created
        for job_name, job in self.run_info[0].items():
            # Parse iteration index from job name: "stage_1[input_5]" → 5
            if "[input_" not in job_name:
                continue

            i = int(job_name.split("[input_")[1].rstrip("]"))

            # Find which stage this job belongs to
            stage_name = job_name.split("[")[0]
            for stage in self.stages:
                if stage.instance_name == stage_name:
                    # Reconstruct the outputs for this iteration
                    output_dir = self._get_output_dir_for_stage(stage_name, i)
                    suffix = self._get_iterator_suffix(i)

                    for tag in stage.output_tags():
                        aliased_tag = stage.get_aliased_tag(tag)
                        base_outputs = stage.find_outputs(
                            self.run_config["output_dir"]
                        )
                        base_path = base_outputs[aliased_tag]
                        output_path = self._make_output_path(
                            base_path, output_dir, suffix
                        )
                        outputs_by_tag[aliased_tag].append(output_path)
                    break

        # Convert to templates
        templated_outputs = {}
        for tag, paths in outputs_by_tag.items():
            # Sort to ensure consistent ordering
            paths.sort()
            templated_outputs[tag] = self._make_template_path(paths)

        return templated_outputs
