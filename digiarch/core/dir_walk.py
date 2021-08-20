import os
from pathlib import Path

for root, dirs, files in os.walk(Path.cwd()):
    print("Root: {}".format(root))
    print("Dirs: {}".format(dirs))
    print("Files: {}".format(files))