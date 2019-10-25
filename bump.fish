function bump
    poetry version $argv[1]
    set new_vers (cat pyproject.toml | grep "^version = \"*\"" | cut -d'"' -f2)
    echo "Writing new version $new_vers to $argv[2]/__init__.py"
    sed -i "s/__version__ = .*/__version__ = \"$new_vers\"/g" $argv[2]/__init__.py
end