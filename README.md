# Installation

`pipx install git+https://github.com/aarhusstadsarkiv/digiarch.git`

# Commands

* [digiarch](#digiarch)
    * [identify](#digiarch-identify)
    * [reidentify](#digiarch-reidentify)
    * [extract](#digiarch-extract)
    * [edit](#digiarch-edit)
        * [action](#digiarch-edit-action)
            * [convert](#digiarch-edit-action-convert)
            * [extract](#digiarch-edit-action-extract)
            * [manual](#digiarch-edit-action-manual)
            * [ignore](#digiarch-edit-action-ignore)
            * [copy](#digiarch-edit-action-copy)
        * [rename](#digiarch-edit-rename)
        * [lock](#digiarch-edit-lock)
        * [processed](#digiarch-edit-processed)
        * [remove](#digiarch-edit-remove)
        * [rollback](#digiarch-edit-rollback)
    * [search](#digiarch-search)
    * [history](#digiarch-history)
    * [doctor](#digiarch-doctor)
    * [upgrade](#digiarch-upgrade)
    * [completions](#digiarch-completions)

## digiarch

```
Usage: digiarch [OPTIONS] COMMAND [ARGS]...

  Identify files and generate the database used by other Aarhus City Archives
  tools.

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  identify     Identify files.
  reidentify   Reidentify files.
  extract      Unpack archives.
  edit         Edit the database.
  search       Search the database.
  history      View events log.
  doctor       Inspect the database.
  upgrade      Upgrade the database.
  completions  Generate shell completions.
```

### digiarch identify

```
Usage: digiarch identify [OPTIONS] ROOT

  Process a folder (ROOT) recursively and populate a files' database.

  Each file is identified with Siegfried and an action is assigned to it.
  Files that need re-identification, renaming, or ignoring are processed
  accordingly.

  Files that are already in the database are not processed.

Options:
  --siegfried-path FILE           The path to the Siegfried executable.  [env
                                  var: SIEGFRIED_PATH; required]
  --siegfried-home DIRECTORY      The path to the Siegfried home folder.  [env
                                  var: SIEGFRIED_HOME; required]
  --siegfried-signature [pronom|loc|tika|freedesktop|pronom-tika-loc|deluxe|archivematica]
                                  The signature file to use with Siegfried.
                                  [default: pronom]
  --actions FILE                  Path to a YAML file containing file format
                                  actions.  [env var: DIGIARCH_ACTIONS]
  --custom-signatures FILE        Path to a YAML file containing custom
                                  signature specifications.  [env var:
                                  DIGIARCH_CUSTOM_SIGNATURES]
  --exclude TEXT                  Glob pattern for file and folder names to
                                  exclude.  [multiple]
  --batch-size INTEGER RANGE      [x>=1]
  --help                          Show this message and exit.
```

### digiarch reidentify

```
Usage: digiarch reidentify [OPTIONS] ROOT [QUERY]

  Re-indentify specific files in the ROOT folder.

  Each file is re-identified with Siegfried and an action is assigned to it.
  Files that need re-identification with custom signatures, renaming, or
  ignoring are processed accordingly.

  For details on the QUERY argument, see the edit command.

  If there is no query, then all files with identification warnings or have no
  PUID or have no action, and that are neither locked nor processed will be
  re-identified.

Options:
  --siegfried-path FILE           The path to the Siegfried executable.  [env
                                  var: SIEGFRIED_PATH; required]
  --siegfried-home DIRECTORY      The path to the Siegfried home folder.  [env
                                  var: SIEGFRIED_HOME; required]
  --siegfried-signature [pronom|loc|tika|freedesktop|pronom-tika-loc|deluxe|archivematica]
                                  The signature file to use with Siegfried.
                                  [default: pronom]
  --actions FILE                  Path to a YAML file containing file format
                                  actions.  [env var: DIGIARCH_ACTIONS]
  --custom-signatures FILE        Path to a YAML file containing custom
                                  signature specifications.  [env var:
                                  DIGIARCH_CUSTOM_SIGNATURES]
  --batch-size INTEGER RANGE      [x>=1]
  --help                          Show this message and exit.
```

### digiarch extract

```
Usage: digiarch extract [OPTIONS] ROOT

  Unpack archives and identify files therein.

  Files are unpacked recursively, i.e., if an archive contains another
  archive, this will be unpacked as well.

  Archives with unrecognized extraction tools will be set to manual mode.

  To see the which files will be unpacked (but not their contents) without
  unpacking them, use the --dry-run option.

Options:
  --siegfried-path FILE           The path to the Siegfried executable.  [env
                                  var: SIEGFRIED_PATH; required]
  --siegfried-home DIRECTORY      The path to the Siegfried home folder.  [env
                                  var: SIEGFRIED_HOME; required]
  --siegfried-signature [pronom|loc|tika|freedesktop|pronom-tika-loc|deluxe|archivematica]
                                  The signature file to use with Siegfried.
                                  [default: pronom]
  --actions FILE                  Path to a YAML file containing file format
                                  actions.  [env var: DIGIARCH_ACTIONS]
  --custom-signatures FILE        Path to a YAML file containing custom
                                  signature specifications.  [env var:
                                  DIGIARCH_CUSTOM_SIGNATURES]
  --dry-run                       Show changes without committing them.
  --help                          Show this message and exit.
```

### digiarch edit

```
Usage: digiarch edit [OPTIONS] COMMAND [ARGS]...

  Edit the files' database.

  The ROOT argument in the edit subcommands is a folder that contains a
  _metadata/files.db database, not the _metadata folder itself.

  The QUERY argument uses a simple search syntax.
  @<field> will match a specific field, the following are supported: uuid,
  checksum, puid, relative_path, action, warning, processed, lock.
  @null and @notnull will match columns with null and not null values respectively.
  @true and @false will match columns with true and false values respectively.
  @like toggles LIKE syntax for the values following it in the same column.
  @file toggles file reading for the values following it in the same column: each
  value will be considered as a file path and values will be read from the lines
  in the given file (@null, @notnull, @true, and @false in files are not supported).
  Changing to a new @<field> resets like and file toggles. Values for the same
  column will be matched with OR logic, while values from different columns will
  be matched with AND logic.

  Every edit subcommand requires a REASON argument that will be used in the
  database log to explain the reason behind the edit.

  Query Examples
  --------------

  @uuid @file uuids.txt @warning @notnull = (uuid = ? or uuid = ? or uuid = ?)
  and (warning is not null)

  @relative_path @like %.pdf @lock @true = (relative_path like ?) and (lock is
  true)

  @action convert @relative_path @like %.pdf %.msg = (action = ?) and
  (relative_path like ? or relative_path like ?)

Options:
  --help  Show this message and exit.

Commands:
  action     Change file actions.
  rename     Change file extensions.
  lock       Lock files.
  processed  Set files as processed.
  remove     Remove files.
  rollback   Roll back edits.
```

#### digiarch edit action

```
Usage: digiarch edit action [OPTIONS] COMMAND [ARGS]...

  Change file actions.

Options:
  --help  Show this message and exit.

Commands:
  convert  Set convert action.
  extract  Set extract action.
  manual   Set manual action.
  ignore   Set ignore action.
  copy     Copy action from a format.
```

##### digiarch edit action convert

```
Usage: digiarch edit action convert [OPTIONS] ROOT QUERY REASON

  Set files' action to "convert".

  The --outputs option may be omitted when using the "copy" tool.

  To lock the file(s) after editing them, use the --lock option.

  To see the changes without committing them, use the --dry-run option.

  For details on the QUERY argument, see the edit command.

Options:
  --tool TEXT     The tool to use for conversion.  [required]
  --outputs TEXT  The file extensions to generate.  [multiple; required for
                  tools other than "copy"]
  --lock          Lock the edited files.
  --dry-run       Show changes without committing them.
  --help          Show this message and exit.
```

##### digiarch edit action extract

```
Usage: digiarch edit action extract [OPTIONS] ROOT QUERY REASON

  Set files' action to "extract".

  To lock the file(s) after editing them, use the --lock option.

  To see the changes without committing them, use the --dry-run option.

  For details on the QUERY argument, see the edit command.

Options:
  --tool TEXT       The tool to use for extraction.  [required]
  --extension TEXT  The extension the file must have for extraction to
                    succeed.
  --lock            Lock the edited files.
  --dry-run         Show changes without committing them.
  --help            Show this message and exit.
```

##### digiarch edit action manual

```
Usage: digiarch edit action manual [OPTIONS] ROOT QUERY REASON

  Set files' action to "manual".

  To lock the file(s) after editing them, use the --lock option.

  To see the changes without committing them, use the --dry-run option.

  For details on the QUERY argument, see the edit command.

Options:
  --reason TEXT   The reason why the file must be processed manually.
                  [required]
  --process TEXT  The steps to take to process the file.  [required]
  --lock          Lock the edited files.
  --dry-run       Show changes without committing them.
  --help          Show this message and exit.
```

##### digiarch edit action ignore

```
Usage: digiarch edit action ignore [OPTIONS] ROOT QUERY REASON

  Set files' action to "ignore".

  Template must be one of:
  * text
  * empty
  * password-protected
  * corrupted
  * duplicate
  * not-preservable
  * not-convertable
  * extracted-archive
  * temporary-file

  The --reason option may be omitted when using a template other than "text".

  To lock the file(s) after editing them, use the --lock option.

  To see the changes without committing them, use the --dry-run option.

  For details on the QUERY argument, see the edit command.

Options:
  --template TEMPLATE  The template type to use.  [required]
  --reason TEXT        The reason why the file is ignored.  [required for
                       "text" template]
  --lock               Lock the edited files.
  --dry-run            Show changes without committing them.
  --help               Show this message and exit.
```

##### digiarch edit action copy

```
Usage: digiarch edit action copy [OPTIONS] ROOT QUERY PUID
                                 {convert|extract|manual|ignore} REASON

  Set files' action by copying it from an existing format.

  Supported actions are:
  * convert
  * extract
  * manual
  * ignore

  If no actions file is give with --actions, the latest version will be
  downloaded from GitHub.

  To lock the file(s) after editing them, use the --lock option.

  To see the changes without committing them, use the --dry-run option.

  For details on the QUERY argument, see the edit command.

Options:
  --actions FILE  Path to a YAML file containing file format actions.  [env
                  var: DIGIARCH_ACTIONS]
  --lock          Lock the edited files.
  --dry-run       Show changes without committing them.
  --help          Show this message and exit.
```

#### digiarch edit rename

```
Usage: digiarch edit rename [OPTIONS] ROOT QUERY EXTENSION REASON

  Change the extension of one or more files in the files' database for the
  ROOT folder to EXTENSION.

  To see the changes without committing them, use the --dry-run option.

  The --replace and --replace-all options will only replace valid suffixes
  (i.e., matching the expression \.[a-zA-Z0-9]+).

  The --append option will not add the new extension if it is already present.

Options:
  --append       Append the new extension.  [default]
  --replace      Replace the last extension.
  --replace-all  Replace all extensions.
  --dry-run      Show changes without committing them.
  --help         Show this message and exit.
```

#### digiarch edit lock

```
Usage: digiarch edit lock [OPTIONS] ROOT QUERY REASON

  Lock files from being edited by reidentify.

  To unlock files, use the --unlock option.

  To see the changes without committing them, use the --dry-run option.

  For details on the QUERY argument, see the edit command.

Options:
  --lock / --unlock  Lock or unlock files.  [default: lock]
  --dry-run          Show changes without committing them.
  --help             Show this message and exit.
```

#### digiarch edit processed

```
Usage: digiarch edit processed [OPTIONS] ROOT QUERY REASON

  Set files as processed.

  To set files as unprocessed, use the --unprocessed option.

  To see the changes without committing them, use the --dry-run option.

  For details on the QUERY argument, see the edit command.

Options:
  --processed / --unprocessed  Set files as processed or unprocessed.
                               [default: processed]
  --dry-run                    Show changes without committing them.
  --help                       Show this message and exit.
```

#### digiarch edit remove

```
Usage: digiarch edit remove [OPTIONS] ROOT QUERY REASON

  Remove one or more files in the files' database for the ROOT folder to
  EXTENSION.

  Using the --delete option removes the files from the disk.

  To see the changes without committing them, use the --dry-run option.

  For details on the QUERY argument, see the edit command.

Options:
  --delete   Remove selected files from the disk.
  --dry-run  Show changes without committing them.
  --help     Show this message and exit.
```

#### digiarch edit rollback

```
Usage: digiarch edit rollback [OPTIONS] ROOT FROM TO REASON

  Roll back edits between two timestamps.

  FROM and TO timestamps must be in the format '%Y-%m-%dT%H:%M:%S' or
  '%Y-%m-%dT%H:%M:%S.%f'.

  Using the --command option allows to restrict rollbacks to specific events
  with the given commands if the timestamps are not precise enough. E.g.,
  "digiarch.edit.rename" to roll back changes performed by the "edit rename"
  command.

  To see the changes without committing them, use the --dry-run option.

Options:
  --command TEXT  Specify commands to roll back.  [multiple]
  --dry-run       Show changes without committing them.
  --help          Show this message and exit.
```

### digiarch search

```
Usage: digiarch search [OPTIONS] ROOT [QUERY]

  Search for specific files in the database.

  Files are displayed in YAML format.

  For details on the QUERY argument, see the edit command.

  If there is no query, then the limit is automatically set to 100 if not set
  with the --limit option.

Options:
  --order-by [relative_path|size|action]
                                  Set sorting field.  [default: relative_path]
  --sort [asc|desc]               Set sorting direction.  [default: asc]
  --limit INTEGER RANGE           Limit the number of results.  [x>=1]
  --help                          Show this message and exit.
```

### digiarch history

```
Usage: digiarch history [OPTIONS] ROOT

  View and search events log.

  The --operation and --reason options supports LIKE syntax with the %
  operator.

  If multiple --uuid, --operation, or --reason options are used, the query
  will match any of them.

  If no query option is given, then the limit is automatically set to 100 if
  not set with the --limit option.

Options:
  --from [%Y-%m-%d|%Y-%m-%dT%H:%M:%S|%Y-%m-%dT%H:%M:%S.%f]
                                  Minimum date of events.
  --to [%Y-%m-%d|%Y-%m-%dT%H:%M:%S|%Y-%m-%dT%H:%M:%S.%f]
                                  Maximum date of events.
  --operation TEXT                Operation and sub-operation.  [multiple]
  --uuid TEXT                     File UUID.  [multiple]
  --reason TEXT                   Event reason.
  --ascending / --descending      Sort by ascending or descending order.
                                  [default: ascending]
  --limit INTEGER RANGE           Limit the number of results.  [x>=1]
  --help                          Show this message and exit.
```

### digiarch doctor

```
Usage: digiarch doctor [OPTIONS] ROOT

  Inspect the database for common issues.

  The current fixes will be applied:
  * Path sanitizing (paths): paths containing any invalid characters (\?%*|"<>,:;=+[]!@) will be renamed with those
      characters removed
  * Duplicated extensions (extensions): paths ending with duplicated extensions will be rewritten to remove
      duplicated extensions and leave only one
  * Check files (files): ensure that all files in the database exist, if not they are removed

  To see the changes without committing them, use the --dry-run option.

Options:
  --fix [paths|extensions|files]  Specify which fixes to apply.
  --dry-run                       Show changes without committing them.
  --help                          Show this message and exit.
```

### digiarch upgrade

```
Usage: digiarch upgrade [OPTIONS] ROOT

  Upgrade the files' database to the latest version of acacore.

  When using --backup, a copy of the current database version will be created
  in the same folder with the name "files-{version}.db". The copy will not be
  created if the database is already at the latest version.

Options:
  --backup / --no-backup  Backup current version.  [default: backup]
  --help                  Show this message and exit.
```

### digiarch completions

```
Usage: digiarch completions [OPTIONS] {bash|fish|zsh}

  Generate tab-completion scripts for your shell.

  The generated completion must be saved in the correct location for it to be
  recognized and used by the shell.

  Supported shells are:
  * bash      Bourne Again Shell
  * fish      Friendly Interactive Shell
  * zsh       Z shell

Options:
  --help  Show this message and exit.
```

