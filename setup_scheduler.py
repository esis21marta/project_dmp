import os
import subprocess

os.chdir("/home/cdsw/project_dmp")
# change path
subprocess.run(["git", "checkout", "develop"])
subprocess.run(["git", "pull"])
subprocess.run(["pip3", "install", "-r", "src/requirements.txt"])
