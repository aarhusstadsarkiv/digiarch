from os import PathLike
from pathlib import Path
from re import match
from sqlite3 import DatabaseError

import yaml
from acacore.database import FilesDB
from acacore.database.upgrade import is_latest
from acacore.models.reference_files import Action
from acacore.models.reference_files import CustomSignature
from acacore.reference_files import get_actions
from acacore.reference_files import get_custom_signatures
from acacore.utils.click import ctx_params
from click import BadParameter
from click import Context
from click import option
from click import Parameter
from click import Path as ClickPath
from pydantic import TypeAdapter


# noinspection PyPep8Naming
class AVIDIndices:
    def __init__(self, avid_dir: Path):
        self.avid_dir = avid_dir

    @property
    def path(self):
        return self.avid_dir / "Indices"

    @property
    def archiveIndex(self) -> Path:
        """Indices/archiveIndex.xml"""
        return self.path / "archiveIndex.xml"

    @property
    def contextDocumentationIndex(self) -> Path:
        """Indices/contextDocumentationIndex.xml"""
        return self.path / "contextDocumentationIndex.xml"

    @property
    def docIndex(self) -> Path:
        """Indices/docIndex.xml"""
        return self.path / "docIndex.xml"

    @property
    def fileIndex(self) -> Path:
        """Indices/fileIndex.xml"""
        return self.path / "fileIndex.xml"

    @property
    def tableIndex(self) -> Path:
        """Indices/tableIndex.xml"""
        return self.path / "tableIndex.xml"


# noinspection PyPep8Naming
class AVIDSchemas:
    def __init__(self, avid_dir: Path):
        self.avid_dir: Path = avid_dir

    @property
    def path(self):
        return self.avid_dir / "Schemas"

    @property
    def archiveIndex(self) -> Path:
        """Schemas/standard/archiveIndex.xsd"""
        return self.path / "standard" / "archiveIndex.xsd"

    @property
    def contextDocumentationIndex(self) -> Path:
        """Schemas/standard/contextDocumentationIndex.xsd"""
        return self.path / "standard" / "contextDocumentationIndex.xsd"

    @property
    def docIndex(self) -> Path:
        """Schemas/standard/docIndex.xsd"""
        return self.path / "standard" / "docIndex.xsd"

    @property
    def fileIndex(self) -> Path:
        """Schemas/standard/fileIndex.xsd"""
        return self.path / "standard" / "fileIndex.xsd"

    @property
    def researchIndex(self) -> Path:
        """Schemas/standard/researchIndex.xsd"""
        return self.path / "standard" / "researchIndex.xsd"

    @property
    def tableIndex(self) -> Path:
        """Schemas/standard/tableIndex.xsd"""
        return self.path / "standard" / "tableIndex.xsd"

    @property
    def XMLSchema(self) -> Path:
        """Schemas/standard/XMLSchema.xsd"""
        return self.path / "standard" / "XMLSchema.xsd"

    @property
    def tables(self) -> dict[int, Path]:
        """Tables/tableN/tableN.xsd"""
        return {
            int(f.name.removeprefix("table")): f.joinpath(f.name).with_suffix(".xsd")
            for f in self.avid_dir.joinpath("Tables").iterdir()
            if f.is_dir() and match(r"table\d+", f.name)
        }


class AVIDDirs:
    def __init__(self, avid_dir: Path):
        self.dir: Path = avid_dir

    @property
    def original_documents(self):
        return self.dir / "OriginalDocuments"

    @property
    def master_documents(self):
        return self.dir / "MasterDocuments"

    @property
    def access_documents(self):
        return self.dir / "AccessDocuments"

    @property
    def documents(self):
        return self.dir / "Documents"

    @property
    def indices(self) -> AVIDIndices:
        """Indices"""
        return AVIDIndices(self.dir)

    @property
    def schemas(self) -> AVIDSchemas:
        """Schemas"""
        return AVIDSchemas(self.dir)

    @property
    def tables(self) -> dict[int, Path]:
        """Tables"""
        return {
            int(f.name.removeprefix("table")): f.joinpath(f.name).with_suffix(".xml")
            for f in self.dir.joinpath("Tables").iterdir()
            if f.is_dir() and match(r"table\d+", f.name)
        }


class AVID:
    def __init__(self, directory: str | PathLike):
        if not self.is_avid_dir(directory):
            raise ValueError(f"{directory} is not a valid AVID directory")

        self.path: Path = Path(directory).resolve()
        self.dirs: AVIDDirs = AVIDDirs(self.path)

    @classmethod
    def is_avid_dir(cls, directory: str | PathLike[str]) -> bool:
        directory = Path(directory)
        if not directory.is_dir():
            return False
        if not (avid_dirs := AVIDDirs(directory)).indices.path.is_dir():
            return False
        if not avid_dirs.schemas.path.is_dir():
            return False
        if not avid_dirs.original_documents.is_dir() and not avid_dirs.documents.is_dir():
            return False
        return True

    @classmethod
    def find_database_root(cls, directory: str | PathLike[str]) -> Path | None:
        directory = Path(directory)
        if directory.joinpath("_metadata", "avid.db").is_file():
            return directory
        if directory.parent != directory:
            return cls.find_database_root(directory.parent)
        return None

    @property
    def metadata_dir(self):
        return self.path / "_metadata"

    @property
    def database_path(self):
        return self.metadata_dir / "avid.db"


def option_avid():
    def _callback(ctx: Context, param: Parameter, value: str | PathLike[str] | None):
        if value is None and (value := AVID.find_database_root(Path.cwd())) is None:
            raise BadParameter(f"No AVID directory found in path {str(Path.cwd())!r}.", ctx, param)
        if not AVID.is_avid_dir(value):
            raise BadParameter(f"Not a valid AVID directory {value!r}.", ctx, param)
        if not (avid := AVID(value)).database_path.is_file():
            raise BadParameter(f"No _metadata/avid.db present in {value!r}.", ctx, param)
        return avid

    return option(
        "--avid-root",
        "avid",
        type=ClickPath(exists=True, file_okay=False, writable=True, readable=True, resolve_path=True),
        default=None,
        callback=_callback,
    )


def option_dry_run():
    return option("--dry-run", is_flag=True, default=False, help="Show changes without committing them.")


def open_database(ctx: Context, avid: AVID) -> FilesDB:
    db = FilesDB(avid.database_path, check_initialisation=False, check_version=True)
    if not db.is_initialised():
        raise BadParameter("Database is not initialised.", ctx, ctx_params(ctx)["avid"])
    try:
        is_latest(db.connection, raise_on_difference=True)
    except DatabaseError as e:
        raise BadParameter(e.args[0], ctx, ctx_params(ctx)["avid"])

    return db


def fetch_actions(ctx: Context, parameter_name: str, file: Path | None) -> dict[str, Action]:
    if file:
        try:
            with file.open() as fh:
                return TypeAdapter(dict[str, Action]).validate_python(yaml.load(fh, yaml.Loader))
        except BaseException:
            raise BadParameter("Invalid actions file.", ctx, ctx_params(ctx)[parameter_name])

    try:
        return get_actions()
    except BaseException as err:
        raise BadParameter(
            f"Cannot download actions. {err.args[0] if err.args else ''}", ctx, ctx_params(ctx)[parameter_name]
        )


def fetch_custom_signatures(ctx: Context, parameter_name: str, file: Path | None) -> list[CustomSignature]:
    if file:
        try:
            with file.open() as fh:
                return TypeAdapter(list[CustomSignature]).validate_python(yaml.load(fh, yaml.Loader))
        except BaseException:
            raise BadParameter("Invalid custom signatures file.", ctx, ctx_params(ctx)[parameter_name])

    try:
        return get_custom_signatures()
    except BaseException as err:
        raise BadParameter(
            f"Cannot download actions. {err.args[0] if err.args else ''}", ctx, ctx_params(ctx)[parameter_name]
        )
