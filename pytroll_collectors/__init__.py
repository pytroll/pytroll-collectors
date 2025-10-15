"""The place where everything starts :)."""
try:
    from pytroll_collectors.version import version  # noqa
except ModuleNotFoundError as err:  # pragma: no cover
    err.add_note("This could mean you didn't install 'pytroll_collectors' properly.")
    err.add_note("Try reinstalling ('pip install').")
    raise
