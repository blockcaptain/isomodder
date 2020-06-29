# mypy: allow-untyped-defs
# Callback can't be annotated because it breaks pycdlib argument inspection.
def write_progress_callback(progress, completed, total, task_id):
    progress.update(task_id, completed=completed, total=total)
