# Databricks notebook source
# MAGIC %md
# MAGIC # Land sample data
# MAGIC
# MAGIC Copies the sample CSV files shipped in the project's `data/` folder into the
# MAGIC DBFS landing directory that Auto Loader watches.

dbutils.widgets.text("landing_path", "")
dbutils.widgets.text("source_data_path", "")

landing_path = dbutils.widgets.get("landing_path")
source_data_path = dbutils.widgets.get("source_data_path").strip()

if not landing_path:
    raise ValueError("landing_path widget must be set")


# Known storage schemes — a path that already has one is left untouched.
_SCHEMES = ("dbfs:", "file:", "s3:", "s3a:", "abfss:", "gs:", "wasbs:")


def _to_fs_path(path: str) -> str:
    """Resolve a workspace path to something dbutils.fs can read.

    """
    if path.startswith(_SCHEMES):
        return path
    if path.startswith("/Workspace"):
        return "file:" + path
    if path.startswith(("/Repos", "/Users", "/Shared")):
        return "file:/Workspace" + path
    return path


def _default_source_data_path() -> str:
    """Locate the project's ``data/`` folder relative to this notebook.
    """
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    nb_path = ctx.notebookPath().get()
    project_root = nb_path.rsplit("/src/setup/", 1)[0]
    return f"file:/Workspace{project_root}/data"


source = _to_fs_path(source_data_path) if source_data_path else _default_source_data_path()
print(f"Source data directory: {source}")
print(f"Landing zone:          {landing_path}")


dbutils.fs.mkdirs(landing_path)

csv_files = [entry for entry in dbutils.fs.ls(source) if entry.name.endswith(".csv")]
if not csv_files:
    raise FileNotFoundError(f"No CSV files found under {source}")

for entry in csv_files:
    dbutils.fs.cp(entry.path, f"{landing_path}/{entry.name}")
    print(f"Copied {entry.name} -> {landing_path}")

print(f"Landed {len(csv_files)} CSV file(s) in {landing_path}")
