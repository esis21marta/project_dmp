# Copyright 2018-present QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

import time

import click
from tqdm import tqdm

try:
    import git
except ModuleNotFoundError:
    click.secho(
        "This script requires gitpython. Try running `pip install gitpython`", fg="red"
    )
    raise


@click.group()
def pushypull():
    """
   """


@pushypull.command()
@click.option("--force/--no-force", default=False)
@click.option("--verify/--no-verify", default=True)
def push(force, verify):
    """Push data to gitlab"""
    repo = git.Repo()
    if repo.active_branch.name == "master":
        print("Not mucking with master...")
        exit(1)

    if not force:
        res = input(
            "Press enter to make a new commit, or ctrl-c to quit and do nothing"
        )
        if res.lower().startswith("y"):
            print("Ok, exiting...")
            return

    for file in repo.index.diff(None):
        repo.index.add([file.a_path])

    commit_args = ["--amend", "--no-edit"]
    if not verify:
        commit_args.append("--no-verify")
    try:
        repo.git.commit(*commit_args)
    except git.GitCommandError as e:
        click.secho("Git error: " + str(e), fg="red")
        return

    repo.git.push("--force")
    print(repo.head.object.hexsha)


@pushypull.command()
def pull():
    """Pull data from gitlab"""
    repo = git.Repo()
    curr_sha = repo.head.object.hexsha
    branch = repo.active_branch
    if repo.active_branch == "master":
        print("Not mucking with master...")
    pbar = tqdm(range(20), desc="Pausing 20 seconds to allow for a push")
    ix = 0
    while True:
        time.sleep(1)
        ix += 1
        if ix > 20 and ix % 5 == 0:
            repo.git.fetch()
            print(curr_sha, branch.tracking_branch().object.hexsha)
            if branch.tracking_branch().object.hexsha != curr_sha:
                break
        pbar.update()
    pbar.close()

    repo.git.reset("--hard", branch.tracking_branch().object.hexsha)
    print("Reset to SHA: ", repo.head.object.hexsha)
    print("Done")


if __name__ == "__main__":
    pushypull()
