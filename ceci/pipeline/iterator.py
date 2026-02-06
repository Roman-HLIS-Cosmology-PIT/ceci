import re
from pathlib import Path


class Iterator:
    """Manages iterator operations for multi-input pipelines.

    Parameters
    ----------
    iterator_id : str
        Name of the iterator variable
    pattern : str
        Regex pattern to match iterator values
    """

    def __init__(self, iterator_id, pattern):
        """Initialize iterator with id and pattern only."""
        self.iterator_id = iterator_id
        self.pattern = pattern

        # Compile regex for efficient job name parsing
        self.regex = re.compile(f"({pattern})$")

    def generate_values(self, file_path):
        """Generate iterator values by discovering files matching a pattern.

        This is a generator that yields iterator values on-demand by scanning
        the filesystem for files matching the pattern.

        Parameters
        ----------
        file_path : str
            File path template with {iterator_id} placeholder
            Example: './inputs/data{field_id}.txt'

        Yields
        ------
        str
            Iterator value extracted from matching file
        """
        prefix, it_part, suffix = self._split_path(file_path)

        root = Path(*prefix) if prefix else Path(".")
        if not root.exists():
            return

        it_regex = self._build_regex(it_part)

        for entry in root.iterdir():
            m = it_regex.match(entry.name)
            if not m:
                continue

            candidate = entry.joinpath(*suffix) if suffix else entry
            if candidate.is_file():
                yield m.group(self.iterator_id)

    def _split_path(self, file_path):
        """Split file_path into prefix, iterator part, and suffix.

        Parameters
        ----------
        file_path : str
            Path template with {iterator_id} placeholder

        Returns
        -------
        tuple
            (prefix_parts, iterator_part, suffix_parts)
        """
        marker = f"{{{self.iterator_id}}}"
        parts = Path(file_path).parts

        hits = [i for i, p in enumerate(parts) if marker in p]
        if len(hits) != 1:
            raise ValueError(
                f"file_path must contain exactly one occurrence of '{marker}'"
            )

        i = hits[0]
        return parts[:i], parts[i], parts[i + 1 :]

    def _build_regex(self, part):
        """Build regex for matching the iterator-containing path part.

        Parameters
        ----------
        part : str
            Path component containing {iterator_id}

        Returns
        -------
        re.Pattern
            Compiled regex pattern
        """
        marker = f"{{{self.iterator_id}}}"

        escaped = re.escape(part)
        placeholder = re.escape(marker)

        regex = escaped.replace(
            placeholder, f"(?P<{self.iterator_id}>{self.pattern})"
        )

        return re.compile("^" + regex + "$")

    def make_job_name(self, stage_name, iterator_value):
        """Create job name using iterator value.

        Parameters
        ----------
        stage_name : str
            Name of the stage
        iterator_value : str
            Value of the iterator for this job

        Returns
        -------
        str
            Job name in format "stage_name{iterator_value}"
        """
        return f"{stage_name}{iterator_value}"

    def parse_job_name(self, job_name):
        """Parse job name to extract stage name and iterator value.

        Parameters
        ----------
        job_name : str
            Job name in format "stage_name{iterator_value}"

        Returns
        -------
        tuple
            (stage_name, iterator_value)
        """
        match = self.regex.search(job_name)

        if match:
            iterator_value = match.group(1)
            stage_name = job_name[: match.start()]
            return stage_name, iterator_value

        return job_name, None

    def expand_template(self, path_template, iterator_value):
        """Expand a template path with a specific iterator value.

        Parameters
        ----------
        path_template : str
            Template path with {iterator_id} placeholder
        iterator_value : str
            Iterator value to substitute

        Returns
        -------
        str
            Concrete path with template substituted
        """
        template_marker = f"{{{self.iterator_id}}}"
        if template_marker in path_template:
            return path_template.replace(template_marker, str(iterator_value))
        return path_template
