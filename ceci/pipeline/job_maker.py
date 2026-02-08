"""
This code has been generated with the help of an AI: Claude 4.5
"""

from .. import minirunner
from pathlib import Path


class JobMaker:
    """Makes Job objects on-demand.

    This allows the pipeline to store lightweight metadata and only create
    full Job objects (with expanded commands) when needed.
    """

    def __init__(self, iterator, stages_config, run_config):
        """Initialize job maker.

        Parameters
        ----------
        iterator : Iterator
            Iterator for expanding templates
        stages_config : dict
            Stage configuration
        run_config : dict
            Run configuration
        """
        self.iterator = iterator
        self.stages_config = stages_config
        self.run_config = run_config

    def create_job(self, stage_metadata, iterator_value):
        """Create a Job object from stage metadata and iterator value.

        Parameters
        ----------
        stage_metadata : dict
            Stage-level metadata (shared across all iterator values):
            - stage: Stage object
            - stage_name: name of stage
            - stage_idx: index of stage
            - cores: number of cores
            - nodes: number of nodes
            - input_templates: dict of input templates
            - pipeline_files: dict of pipeline files (previous outputs)
            - output_dir: base output directory
            - stage_execution_config: StageExecutionConfig
            - base_outputs: dict of base output paths
        iterator_value : str
            Iterator value for this specific job

        Returns
        -------
        Job
            Fully constructed Job object ready to run
        """
        stage = stage_metadata["stage"]

        # Create job name
        job_name = self.iterator.make_job_name(
            stage_metadata["stage_name"], iterator_value
        )

        # Build input files by expanding templates
        files_for_this_run = {}

        # Expand templates from pipeline_files (previous stage outputs)
        for tag, path_template in stage_metadata["pipeline_files"].items():
            files_for_this_run[tag] = self.iterator.expand_template(
                path_template, iterator_value
            )

        # Add inputs for this iteration
        for tag, template in stage_metadata["input_templates"].items():
            path = self.iterator.expand_template(template, iterator_value)
            files_for_this_run[tag] = path

        # Build outputs
        outputs = {}
        for tag in stage.output_tags():
            aliased_tag = stage.get_aliased_tag(tag)
            base_path = stage_metadata["base_outputs"][aliased_tag]
            output_path = self._make_output_path(
                base_path, stage_metadata["output_dir"], iterator_value
            )
            outputs[aliased_tag] = output_path

        # Generate command
        sec = stage_metadata["stage_execution_config"]
        cmd = sec.generate_full_command(
            files_for_this_run, outputs, self.stages_config
        )

        # Create Job object
        return minirunner.Job(
            job_name,
            cmd,
            cores=stage_metadata["cores"],
            nodes=stage_metadata["nodes"],
        )

    def _make_output_path(self, base_path, output_dir, suffix):
        """Create output path with proper directory and suffix."""
        p = Path(base_path)
        new_filename = f"{p.stem}{suffix}{p.suffix}"
        return str(Path(output_dir) / new_filename)
