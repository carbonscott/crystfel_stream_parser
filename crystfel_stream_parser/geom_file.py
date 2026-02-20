"""Standalone reader for CrystFEL .geom geometry files.

Parses key-value pairs using the same delimiters as the stream parser
(both ``=`` and ``:`` forms) and returns a flat dict suitable for
``CheetahConverter.__init__()``.
"""

import re


# Patterns matching the two key-value styles found in CrystFEL files.
# ``=`` form:  p0a0/min_fs = 0
# ``:`` form:  Image filename: /path/to/file.h5
_KV_EQ_RE = re.compile(r"^(.+?)\s+=\s+(.+)$")
_KV_COLON_RE = re.compile(r"^(.+?):\s+(.+)$")


def read_geom_file(path):
    """Read a CrystFEL ``.geom`` file and return a flat key-value dict.

    The returned dict has the same structure that
    ``CheetahConverter.__init__()`` expects, e.g.::

        {
            'p0a0/min_fs': '0',
            'p0a0/corner_x': '79.84',
            ...
        }

    Global settings (``clen``, ``res``, ``photon_energy``, etc.) are
    included as well; ``CheetahConverter`` simply ignores keys that don't
    match its panel regex patterns.

    Args:
        path: Path (str or pathlib.Path) to a ``.geom`` file.

    Returns:
        dict: Mapping of geometry keys to their string values.
    """
    geom_dict = {}

    with open(path, "r") as fh:
        for line in fh:
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith(";"):
                continue

            # Try ``=`` delimiter first (most common in .geom files)
            m = _KV_EQ_RE.match(line)
            if m is not None:
                key = m.group(1).strip()
                value = m.group(2).strip()
                geom_dict[key] = value
                continue

            # Fall back to ``:`` delimiter
            m = _KV_COLON_RE.match(line)
            if m is not None:
                key = m.group(1).strip()
                value = m.group(2).strip()
                geom_dict[key] = value

    return geom_dict
