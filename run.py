import runpy
import sys
from os import getcwd
from os.path import join

sys.path.append(join(getcwd(), "src"))

# Execute a module by its full module name
runpy.run_module("hdx.scraper.worldpop", run_name="__main__")
