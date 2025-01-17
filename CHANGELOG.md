# Changelog

## v5.2.0

### New Features

* `finalize doc-collections` command to rearrange files in Documents into docCollection directories.
* `info` command to print general information about the database

### Fixes

* Fix MSG extraction failing sometimes when attachments were incorrectly parsed as MSG files

## v5.1.0

### New Features

* `edit original puid` and `edit master puid` commands to set the PUID of original and master files respectively
* `manual extract` command to add manually extracted files
* `manual convert` command to add manually converted files

## v5.0.3

### Fixes

* Fix files being overwritten causing an error when MSG and TNEF files contained more than one attachment with the same
  name.

## v5.0.2

### Changes

* When using the `@file` operator in queries, the values are matched exactly with the [SQL
  `IN` operator](https://sqlite.org/src/doc/tip/src/in-operator.md).
    * The `@like` operator no longer has any effect when use in conjunction with `@file`

## v5.0.1

### Changes

* Use acacore 4.1.1
* `edit master processed` can now set processed status of access and statutory targets separately

### Fixes

* Fix issue with rollback getting interrupted before finishing if a run contained unhandled events

## v5.0.0

Complete overhaul of digiarch to work with the entire AVID folder and handle files across document types (original,
master, access, and statutory).

General structure is the same but some commands have been update to support the different document types.

### New Features

* Handle original, master, access, and statutory documents
* Automatically detect root AVID folder
* Import a database created with v4 of digiarch
* New `edit master` commands to handle master files

### Changes

* Removed duplicated functions like `reidentify`, query arguments are used instead to run/rerun a command on specific
  files
* Improved file identification and extraction to be faster and more resilient
* Overhauled and safer rollback command
* Improved filename sanitization when extracting files

### Fixes

* Fix file name length error when running extract on some MSG files whose attached files had extra-long
  names [#748](https://github.com/aarhusstadsarkiv/digiarch/issues/748)

## v4.1.12

### Changes

* Use acacore 3.3.3

## v4.1.11

### Changes

* Use acacore 3.3.2

## v4.1.10

### Fixes

* Fix `doctor` command adding underscores to files that required no fixing when sanitizing paths

## v4.1.9

### Changes

* Use acacore 3.3.1

## v4.1.8

### Changes

* Use acacore 3.3.0

## v4.1.7

### Changes

* Use acacore 3.2.0

## v4.1.6

### Changes

* Use acacore 3.1.1
* `extract` for TNEF files does not create an HTML/RTF/TXT file for the body

### Fixes

* Fix `reidentify` using the wrong UUID when an error occurred during identification, causing it to not updated the file

## v4.1.5

### Fixes

* Fix `parent` column being sometimes reset when running `reidentify` on files extracted from archives

## v4.1.4

### Changes

* When running `reidentify`, the lock and processed values of files are preserved
* When running `reidentify` without a query, both locked and processed files are ignored, and files without an action
  are selected
* `extract` command removes empty folders when it is done
* When `extract` encounters an error and sets the file to "manual", the file is also locked

## v4.1.3

### New Features

* Add webarchive extractor

## v4.1.2

### Changes

* Use acacore 3.1.0

## v4.1.1

### Changes

* Use acacore 3.0.11

## v4.1.0

### New Feature

* New `edit processed` command to set files' processed status

### Changes

* Use acacore 3.0.10

### Fixes

* Fix `PIL.UnidentifiedImageError` causing `identify` and `reidentify` to stop

## v4.0.1

### Changes

* Use acacore 3.0.9
    * Fix #723

### Fixes

* Fix BMP images not being properly checked
    * Caused by imagesize library not supporting them

## v4.0.0

### New Features

* Overhauled query syntax for edit, reidentify, and search commands
    * `@<field>` will match a specific field, the following are supported: uuid, checksum, puid, relative_path, action,
      warning, processed, lock.
    * `@null` and `@notnull` will match columns with null and not null values respectively.
    * `@true` and `@false` will match columns with true and false values respectively.
    * `@like` toggles LIKE syntax for the values following it in the same column.
    * `@file` toggles file reading for the values following it in the same column: each value will be considered as a
      file path and values will be read from the lines in the given file (`@null`, `@notnull`, `@true`, and `@false` in
      files are not supported).
    * Changing to a new `@<field>` resets like and file toggles. Values for the same column will be matched with OR
      logic, while values from different columns will be matched with AND logic.

### Changes

* `extract` sanitized paths of extracted files and saves the originals to history with operation "digiarch.extract:
  rename"
* `edit remove` deletes empty parent directories if the `--delete` option is used

## v3.3.0

### New Features

* `search` command to search files and display them
    * Displays results in YAML format
    * Uses the same selectors as the `edit` commands
    * Supports sorting by relative path, size, and action (both ascending and descending)
* `edit action` commands have a new `--lock` option that locks the files after editing them
    * The default behaviour is to _not_ lock the files
* `history`  command has a new `--limit` option

### Changes

* When running `extract`, unknown errors are logged with the file's uuid
    * The event is not logged to the database, as it is already done with the `end` operation
* `extract` does not automatically add the .msg extension to extracted message attachments, as they could be EML as well
* Improved list of invalid characters for filenames:
    * \\#%&{}[]<>*?/$!'`":@+|=

### Fixes

* Fix `extract` failing on MSG/EML files that contained a message attachments without a filename
    * The subject is used, if available, otherwise `attachment-{n}` is used instead, where n is the index of said
      attachment

## v3.2.16

### Fixes

* Fix issues when extracting attachments from MSG/EML files with forward slashes in the attachment file name

## v3.2.15

### Fixes

* Fix issue with extract when file is already found

## v3.2.14

### Fixes

* Fix `doctor` command `--fix` option not allowing "files" value

## v3.2.13

### New Features

* Add `files` fix to `doctor` command
    * Ensures that all files in the database exist, if not they are removed
* `edit rollback` supports `doctor` commands

### Changes

* `doctor` command events that signal a rename have .rename in their operation name

## v3.2.12

### New Features

* `completions` command generates completions scripts for Zsh, Bash, and Fish shells
* `edit` commands (and others using identifiers) now accept `@null` as a valid value to match `NULL` fields

### Changes

* Use acacore 3.0.8
    * v3.0.7 contained a critical error in the database upgrade function causing `Files.action_data` to be set to
      `NULL`

## v3.2.11

### Changes

* Use acacore 3.0.7

## v3.2.10

### Changes

* Folders for extracted files created by `extract` use the UUID of the archive file
    * Uses format `_{uuid}`
    * Reduces length of nested file paths
    * Is still unique to that folder

## v3.2.9

### Changes

* Use Acacore 3.0.6

## v3.2.8

### Changes

* Show start event for `upgrade` immediately

### Fixes

* Fix MSG attachments of MSG files not using the proper extension
* Fix `upgrade` adding an end event to the history table only when no update occurred

## v3.2.7

### Fixes

* Fix issue with MSG empty attachments
* Fix issue with MSG attachment bytes data sometimes being interpreted as a string by extract_msg causing it through a
  `FileNotFoundError`

## v3.2.6

### Fixes

* Fix incorrect handling of MSG attachments in MSG files

## v3.2.5

### Changes

* Use acacore 3.0.5
    * Fix some edge cases with `edit rename` and `doctor` failing when a file had multiple extensions

### Fixes

* Fix `doctor` extension deduplication not working on some system where the SQLite reverse function was not available

## v3.2.4

### Fixes

* Fix error when extracting HTML and RTF body where they were sometimes None
* Support signed MSG and signed MSG attachments for extraction

## v3.2.3

### New Features

* `edit rollback` supports extract events
    * Archive files are reset to the extract action
    * Extracted files are removed from the database and the file system

### Fixes

* Fix extract events not being saved in History table

## v3.2.2

### Changes

* Reidentify resets `processed` column to `False`

## v3.2.1

### New Features

* Use acacore 3.0.4
* Added "msg" tool to extract MSG files
* Support `extract.on_success`
* `--exclude` option for `identify` to exclude files or folders with globbing patterns

### Changes

* Extracted files now use the "extracted-archive" template when they are set to "ignore"

## v3.2.0

### New Features

* `extract` command to extract archives
    * If the extract tool can't be found, the file is skipped and a warning messages is displayed
    * If the file is encrypted, then the file is set to "ignore" action with template "password-protected"
        * Detection of encrypted archives with Patool is experimental and needs testing
    * If a file should not be preserved, it is set to ignore
    * If other errors occur during extraction, the file is set to manual with reason set to the exception's message

### Changes

* Improved docstrings and help messages

## v3.1.0

### Fixes

* Fix missing help from `history` command when running it without arguments

### Changes

* Improved error messages when downloading actions and custom signatures
* `edit remove` command uses a different sub-operation when deleting files so that they can be automatically ignored by
  rollback

### New Features

* `edit action copy` command to copy an action from an existing format

## v3.0.0

### Changes

* Use a modular structure for commands and subcommands
* Overhauled `edit action` using subcommands and named options for each field
* Added extensions deduplication to `doctor` command
* Improved rollback
* Simplified history events
* Improved handling of exceptions and argument errors

## v2.1.1

### New Features

* `--data-puid` option in `edit action` command allows copying data from an existing identifier in the reference files
    * Fix issues #692
    * If the identifier is not found or the action argument is not found in the data, a `KeyError` exception is raised

### Fixes

* Fix `--id-files` option not working with `edit lock` command

## v2.1.0

### New Features

* Add `edit lock` command to lock specific files
    * Can be rolled back to the previous value with `edit rollback`

## v2.0.2

### New Features

* The `upgrade` command backs up the database file before performing the upgrade
    * Can be ignored with the `--no-backup` option
* The `edit remove` command can delete files from the disk as well with the `--delete` option

## v2.0.1

### Changes

* Use acacore 2.0.1
    * Fix upgrade issues

## v2.0.0

### Changes

* Update to acacore 2.0.0
    * Python 3.11
    * Simpler logging of events
    * Database version checks
* Siegfried batching
    * Files are identified in batches
    * Defaults to 100 files per batch

### New Features

* `doctor` command to fix common database issues
* `upgrade` command to upgrade the database to the latest version

## v1.5.0

### New Features

* `reidentify` command
    * Allows running identification process again on specific files
    * Files are selected with the same system as the edit commands
* `history` command
    * Allows viewing and searching the events log
    * Can search by:
        * time (from and/or to)
        * uuid (allows multiple)
        * operation (LIKE with % only, allows multiple)
        * reason (LIKE, allows multiple)
* `edit rename` accepts an empty extension
    * To set an empty extension, spaces must be used (e.g., `" "`)
    * When used with the `--replace` and `--replace-all` options, existing extensions are removed

### Changes

* Stricter extension patterns in `edit rename`
    * Only allowed characters are a-z, A-Z, and 0-9

## v1.4.0

### New Features

* `edit rollback` command
    * Undo other edit operations
    * Must select a start and end time for history events
* `--dry-run` option for `edit rename`
    * Show changes without committing them

### Changes

* `edit rename` uses replace mode options instead of an f-string
    * `--replace` replaces the last suffix with the new extension (default)
    * `--replace-all` replaces all valid suffixes (matching the expression `\.[^/<>:"\\|?*\x7F\x00-\x20]+`) with the new
      extension
    * `--append` appends the new extension if it is not already there

## v1.3.0

### New Features

* `edit rename` command
    * Change the extension of files
    * Uses the same selector options as the other `edit` commands
    * Ignores changes that would duplicate existing extensions or not alter them
    * New extensions can be formatted with:
        * `suffix` the last extension of the file
        * `suffixes` all the extensions of the file, used for append mode (e.g., `{suffixes}.ext` will change "
          file.tar.gz" to "file.tar.gz.ext")

### Changes

* Added docstring to `edit action` and `edit remove` commands
* `--siegfried-path` can be set with `SIEGFRIED_PATH` environment variable
* `--siegfried-home` can be set with `SIEGFRIED_HOME` environment variable

## v1.2.0

### New Features

* Add `--id-files` option to edit commands
    * Interpret IDs as files from which to read the IDs
    * Each line is considered a separate ID
    * Blank lines are ignore
    * All IDs are stripped of newlines, carriage return, and tab characters, but not spaces

### Changes

* `--no-update-siegfried-signature` option is now the default

### Fixes

* Fix error in `edit remove` command when using `--path-like`
    * Was using the like statement to delete files instead of their UUID
* Fix error in `edit action` command
    * SQLite cursor was rewinding to start because INSERT statements were executed in-between iteration steps

### Dependencies

* acacore 1.2.0

## v1.1.1

### New Features

* LIKE matches for paths
    * Added new `--path-like` option to match edit IDs with LIKE statements

### Changes

* All files matching the given IDs are edited, not just the first one
* Removed Siegfried signature update from test workflow
    * Is already present in test folder
* Updated PRONOM signature file for Siegfried

## v1.1.0

### New Features

#### edit remove

* Added new `edit remove` command to remove files by UUID, path, checksum, PUID, or warning
    * Can be used to re-identify files

### Changes

* Both digiarch's and acacore's versions are saved with the "start" event

### Fixes

* Fix non-matching history events for edit action

## v1.0.7

### Fixes

* Fix traceback of identification errors not being saved in History table

### Dependencies

* Use acacore 1.1.4

## v1.0.6

### Dependencies

* Use acacore 1.1.3

## v1.0.5

### Edit

#### Action

* Allow to use different identifiers than UUID
    * uuid
    * puid
    * relative path
    * checksum
    * warnings
* The history event contains both the previous and new action

## v1.0.4

### Identify

* Improve hadling of exceptions
    * `OSError` and `IOError` are always raised
    * `Exception`, `UnidentifiedImageError`, `DecompressionBombError` are always caught
* Increase maximum size of images before Pillow raises a `DecompressionBombError`

### Other

* Improve end events in history by using the exception repr value in the data column, or None if the program ended with
  no errors

## v1.0.3

### Fixes

* Fix incorrect handling of action data when an `UnidentifiedImageError` exception was caught

### Changes

* `UnidentifiedImageError` exceptions are logged

### Tests

* Add corrupt GIF to test files

## v1.0.2

### Identify

* Handle `UnidentifiedImageError` exception by setting the file to action to "manual"
* Add `--siegfried-home` option to set the folder that contains the signature files

### Dependencies

* Use acacore 1.1.1

### Actions

* Automatically build necessary wheel files and save them as a release on new pushed tags

## v1.0.1

### Edit Action

* New command to change an action
* Can optionally specify new data to be used in action data column

### Dependencies

* Use acacore 1.0.2

## v1.0.0 - Integrate With acacore

### Changes

* Use acacore to handle database and file identification
* Remove all unnecessary files and dependencies

### Dependencies

* Use acacore 1.0.1

## 0.9.23

* added which version of DRIOD we use to the log
* makes sure we use most / all of the avaivable info given by `sf`

## 0.9.22a

* added ability to get reference files version and is printing it to stdout

## 0.9.22

* added check to ensure updates of the changelog

## 0.9.21

* fixed missing identification of aca-fmt/17 (MapInfo Map Files)

## 0.9.20

* added x-fmt/111 to signatures that we re-identify, as Mapinfo TAB files are identified as such
* added aca-fmt/19 (MapInfo TAB files) to list of custom signatures

## 0.9.19

* added list of puids that we have to identify with our custom signatures even though Siegfried identified them.
  Currently "fmt/111".
* added aca-fmt/18 (Lotus Aprroach View File) to custom_signatures.json

## 0.9.18

* added signature for 5 versions of Microsoft Access Database
