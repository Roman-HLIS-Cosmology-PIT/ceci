import re
from pathlib import Path


def split_path(file_path, iterator_id):
    """
    Split file_path into:
      - fixed prefix parts
      - the part containing {iterator_id}
      - fixed suffix parts
    """
    marker = "{" + iterator_id + "}"
    parts = Path(file_path).parts

    hits = [i for i, p in enumerate(parts) if marker in p]
    if len(hits) != 1:
        raise ValueError(
            "file_path must contain exactly one occurrence of "
            f"'{{{iterator_id}}}'"
        )

    i = hits[0]
    return parts[:i], parts[i], parts[i + 1 :]


def build_iterator_regex(part, pattern, iterator_id):
    """
    Build a regex matching the iterator-containing path part
    """
    marker = "{" + iterator_id + "}"

    escaped = re.escape(part)
    placeholder = re.escape(marker)

    regex = escaped.replace(placeholder, f"(?P<{iterator_id}>{pattern})")

    return re.compile("^" + regex + "$")


def iter_matching_files(cfg):
    """
    Efficiently iterate over matching files.

    Yields dicts with:
      - file: Path
      - iterator: extracted iterator value
      - pattern: raw regex pattern (unchanged)
    """
    iterator_id = cfg["iterator_id"]
    pattern = cfg["pattern"]
    file_path = cfg["file_path"]

    prefix, it_part, suffix = split_path(file_path, iterator_id)

    root = Path(*prefix)
    if not root.exists():
        return

    it_regex = build_iterator_regex(it_part, pattern, iterator_id)

    for entry in root.iterdir():
        m = it_regex.match(entry.name)
        if not m:
            continue

        candidate = entry.joinpath(*suffix)
        if candidate.is_file():
            yield {
                "file": candidate,
                "iterator": m.group(iterator_id),
            }
