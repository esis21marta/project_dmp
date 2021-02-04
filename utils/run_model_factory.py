"""
Integration tester for the model factory
----------------------------------------

This is a little utility, which helps me in a few ways:

1. It runs all the right model factory things
2. It displays both the **task status** and the **log messages**  on one
   screen. Super useful!
3. It saves the results of the executions  into text files so that I can
   check them later.

The way to use it is simple: run `python model_factory_integration_test.py`
and it will do it's thing!

There are a few things you can configure,  either in code or the command
line.

1. You can add new tasks. Each should have a name, a command,  and where
   to log the output to.
2. When running, you can pass tasks by name,  so that you can run only a
   subset of tasks. If no task is passed, they're all run.
"""
import curses
import os
import pathlib
import subprocess
import textwrap
from typing import List

import attr
import click


class Task:
    def __init__(self, name: str, base_command: List[str]):
        self.base_command = base_command
        self.name = name

    @property
    def command(self):
        return self.base_command

    @property
    def output_file(self):
        return pathlib.Path(os.path.join("logs", f"{self.name}.txt"))


class ModelFactoryTask:
    fast = False

    def __init__(self, name, env):
        self.env = env
        self.name = name

    @property
    def command(self):
        if self.fast:
            extra_conf = (
                "n_rows_to_sample:10_000,use_pai_logging:True,n_features_to_sample:100"
            )
        else:
            extra_conf = "use_pai_logging:True"

        return [
            "kedro",
            "run",
            "--env",
            self.env,
            "--pipeline",
            "model_factory",
            "--params",
            extra_conf,
        ]

    @property
    def output_file(self):
        return pathlib.Path(os.path.join("logs", f"{self.name}.txt"))


tasks = [
    Task(
        "test",
        ["python", "-c", "import time; print('hi'); time.sleep(1); print('bye!')"],
    ),
    ModelFactoryTask("churn", "model_factory/churn/example"),
    ModelFactoryTask(
        "inlife-fg_lapser_part1_segment_1_myt",
        "model_factory/inlife/fg_lapser_part1_segment_1_myt",
    ),
    ModelFactoryTask(
        "inlife-fg_lapser_part1_segment_1_sms",
        "model_factory/inlife/fg_lapser_part1_segment_1_sms",
    ),
    ModelFactoryTask(
        "inlife-fg_lapser_part1_segment_3_myt",
        "model_factory/inlife/fg_lapser_part1_segment_3_myt",
    ),
    ModelFactoryTask(
        "inlife-hvb_lapser_b_decile_9", "model_factory/inlife/hvb_lapser_b_decile_9"
    ),
    ModelFactoryTask(
        "inlife-ssm_hvc_lapser_a_lp5c_apr",
        "model_factory/inlife/ssm_hvc_lapser_a_lp5c_apr",
    ),
    ModelFactoryTask(
        "inlife-ssm_hvc_lapser_a_lp5d_apr",
        "model_factory/inlife/ssm_hvc_lapser_a_lp5d_apr",
    ),
    ModelFactoryTask(
        "inlife-ssm_hvc_lapser_a_sp5a_apr",
        "model_factory/inlife/ssm_hvc_lapser_a_sp5a_apr",
    ),
    ModelFactoryTask(
        "inlife-ssm_hvc_lapser_a_sp5b_apr",
        "model_factory/inlife/ssm_hvc_lapser_a_sp5b_apr",
    ),
    ModelFactoryTask(
        "inlife-ssm_hvc_lapser_a_sp5c_apr",
        "model_factory/inlife/ssm_hvc_lapser_a_sp5c_apr",
    ),
    ModelFactoryTask(
        "inlife-ssm_rgb_tipe_a_segment_a",
        "model_factory/inlife/ssm_rgb_tipe_a_segment_a",
    ),
    ModelFactoryTask(
        "inlife-ssm_rgb_tipe_a_segment_b",
        "model_factory/inlife/ssm_rgb_tipe_a_segment_b",
    ),
    ModelFactoryTask(
        "inlife-ssm_rgb_tipe_a_segment_c",
        "model_factory/inlife/ssm_rgb_tipe_a_segment_c",
    ),
    ModelFactoryTask("inlife-ssm_thrifty_2b", "model_factory/inlife/ssm_thrifty_2b",),
    ModelFactoryTask("inlife-ssm_thrifty_2d", "model_factory/inlife/ssm_thrifty_2d",),
    ModelFactoryTask("inlife-ssm_thrifty_2g", "model_factory/inlife/ssm_thrifty_2g",),
    ModelFactoryTask("inlife-ssm_thrifty_3f", "model_factory/inlife/ssm_thrifty_3f",),
    ModelFactoryTask("inlife-ssm_thrifty_3h", "model_factory/inlife/ssm_thrifty_3h",),
    ModelFactoryTask(
        "inlife-SSM_4GScaleUp8b_Path2Churn02_SMSApp",
        "model_factory/inlife/SSM_4GScaleUp8b_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "inlife-SSM_4GScaleUp4a_Path2Churn02_SMSApp",
        "model_factory/inlife/SSM_4GScaleUp4a_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "inlife-SSM_4GScaleUp1_Path2Churn02_SMSApp",
        "model_factory/inlife/SSM_4GScaleUp1_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "inlife-SSM_4GScaleUp6a_Path2Churn02_SMSApp",
        "model_factory/inlife/SSM_4GScaleUp6a_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "inlife-SSM_4GScaleUp3a_Path2Churn02_SMSApp",
        "model_factory/inlife/SSM_4GScaleUp3a_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "inlife-SSM_4GScaleUp3b_Path2Churn08_4GBonus_SMSApp",
        "model_factory/inlife/SSM_4GScaleUp3b_Path2Churn08_4GBonus_SMSApp",
    ),
    ModelFactoryTask(
        "inlife-SSM_4GScaleUp7a_Path2Churn02_SMSApp",
        "model_factory/inlife/SSM_4GScaleUp7a_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "inlife-SSM_4GScaleUp2_Path2Churn02_SMSApp",
        "model_factory/inlife/SSM_4GScaleUp2_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "inlife-SSM_4GScaleUp5_Path2Churn08_4GBonus_SMSApp",
        "model_factory/inlife/SSM_4GScaleUp5_Path2Churn08_4GBonus_SMSApp",
    ),
    ModelFactoryTask(
        "inlife-AAGM_CMP_THRIFTY-2ND-SEG2_2A",
        "model_factory/inlife/AAGM_CMP_THRIFTY-2ND-SEG2_2A",
    ),
    ModelFactoryTask(
        "inlife-AAGM_CMP_THRIFTY-2ND-SEG2_2C",
        "model_factory/inlife/AAGM_CMP_THRIFTY-2ND-SEG2_2C",
    ),
    ModelFactoryTask(
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4H-1",
        "model_factory/inlife/AAGM_CMP_INLIFE-DISCHUNT-SEG4_4H-1",
    ),
    ModelFactoryTask(
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4I-1",
        "model_factory/inlife/AAGM_CMP_INLIFE-DISCHUNT-SEG4_4I-1",
    ),
    ModelFactoryTask(
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4H_4I-1",
        "model_factory/inlife/AAGM_CMP_INLIFE-DISCHUNT-SEG4_4H_4I-1",
    ),
    ModelFactoryTask(
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4A-1",
        "model_factory/inlife/AAGM_CMP_INLIFE-DISCHUNT-SEG4_4A-1",
    ),
    ModelFactoryTask(
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4B-1",
        "model_factory/inlife/AAGM_CMP_INLIFE-DISCHUNT-SEG4_4B-1",
    ),
    ModelFactoryTask(
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4C-1",
        "model_factory/inlife/AAGM_CMP_INLIFE-DISCHUNT-SEG4_4C-1",
    ),
    ModelFactoryTask(
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4A_4B_4C-1",
        "model_factory/inlife/AAGM_CMP_INLIFE-DISCHUNT-SEG4_4A_4B_4C-1",
    ),
    ModelFactoryTask(
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4D-1",
        "model_factory/inlife/AAGM_CMP_INLIFE-DISCHUNT-SEG4_4D-1",
    ),
    ModelFactoryTask(
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4E-1",
        "model_factory/inlife/AAGM_CMP_INLIFE-DISCHUNT-SEG4_4E-1",
    ),
    ModelFactoryTask(
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4D_4E-1",
        "model_factory/inlife/AAGM_CMP_INLIFE-DISCHUNT-SEG4_4D_4E-1",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp1_Path2Churn02_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp1_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp2_Path2Churn02_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp2_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp3a_Path2Churn02_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp3a_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp3b_Path2Churn08_4GBonus_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp3b_Path2Churn08_4GBonus_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp4a_Path2Churn02_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp4a_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp4b_GrabnGo01_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp4b_GrabnGo01_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp5_Path2Churn08_4GBonus_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp5_Path2Churn08_4GBonus_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp6a_Path2Churn02_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp6a_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp6b_GrabnGo01_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp6b_GrabnGo01_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp7a_Path2Churn02_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp7a_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp7b_GrabnGo01_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp7b_GrabnGo01_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp8a_GrabnGo01_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp8a_GrabnGo01_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp8b_Path2Churn02_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp8b_Path2Churn02_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp8c_GrabnGo06_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp8c_GrabnGo06_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp8d_VoiceUL_4GB4G_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp8d_VoiceUL_4GB4G_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp9_GrabnGo01_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp9_GrabnGo01_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-SSM_4GScaleUp10_GrabnGo01_SMSApp",
        "model_factory/fourg/SSM_4GScaleUp10_GrabnGo01_SMSApp",
    ),
    ModelFactoryTask(
        "fourg-ANMAR_HVC_Path_to_Churn_2a_scaleup",
        "model_factory/fourg/ANMAR_HVC_Path_to_Churn_2a_scaleup",
    ),
    ModelFactoryTask(
        "fourg-ANMAR_HVC_Path_to_Churn_8b_scaleup",
        "model_factory/fourg/ANMAR_HVC_Path_to_Churn_8b_scaleup",
    ),
]


def execute(cmd):
    popen = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, universal_newlines=True, stderr=subprocess.STDOUT
    )
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)


@attr.s
class RunState:
    upper_lines = attr.ib()
    lower_lines = attr.ib()

    def display(self, stdscr):
        # Clear screen and set up breaks and lines
        stdscr.clear()
        win_break = curses.LINES // 5 * 4
        line_ix = 0

        # Print out all upper lines
        for line in self.upper_lines[-win_break:]:
            stdscr.addstr(line_ix, 0, line)
            line_ix += 1

        # Print a bar of '-'
        assert line_ix <= win_break
        line_ix = win_break
        stdscr.addstr(line_ix, 0, "-" * curses.COLS, curses.color_pair(1))
        line_ix += 1

        # Print the lower lines. Dodgily pick the color based on what is
        # in the text. This would fail if the text appears in the task's
        # name, for example...
        lower_win = curses.LINES - 1 - win_break
        for line in self.lower_lines[-lower_win:]:
            pair = curses.color_pair(1)
            if "Fail" in line:
                pair = curses.color_pair(2)
            if "OK" in line:
                pair = curses.color_pair(3)
            stdscr.addstr(line_ix, 0, line, pair)
            line_ix += 1


def find_task(key):
    for t in tasks:
        if t.name == key:
            return t
    names = [t.name for t in tasks]
    raise KeyError(f"Cannot find task named {key} in {names}")


def run_tasks(stdscr, task_keys):
    state = RunState([], [])
    hasfail = False
    curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_GREEN, curses.COLOR_BLACK)

    for key in task_keys:
        taskspec = find_task(key)
        task_name = f"   Running task {taskspec.name} "
        assert curses.COLS > len(task_name) + 1
        ndots = curses.COLS - len(task_name) - 10

        # Insert the task name info into the state object.  This happens
        # before the program starts, and is basically just to keep us up
        # to date with what the program is doing
        state.upper_lines.extend(
            [f">>> Starting task {taskspec.name} -- {taskspec.command}"] * 5
        )
        state.lower_lines.append(task_name + "." * ndots)
        state.display(stdscr)
        stdscr.refresh()

        # First, open the output file
        #   then, execute the task and for each output line:
        #      - write it to the output file
        #      - text-wrap it and write it to the display lines
        #      - and then trigger the display & refresh
        print(taskspec.output_file)
        with taskspec.output_file.open("w") as f:
            try:
                runner = execute(taskspec.command)
                for stdout_line in runner:
                    f.write(stdout_line)
                    lines = textwrap.wrap(stdout_line, width=curses.COLS)
                    state.upper_lines.extend(lines)
                    state.display(stdscr)
                    stdscr.refresh()

                # At completion, print out some nice messages.
                #
                # We update the last line (which has "task name ...." in
                # it) to include a tick if everything was good.  Or,  if
                # there was an error, update with a cross.
                #
                # Also, we write the status out to the files as well
                f.write("\nFINISHED SUCCESSFULLY\n")
                state.lower_lines[-1] += " ✔ OK!"
            except subprocess.CalledProcessError:
                f.write("\nFAILURE\n")
                state.lower_lines[-1] += "❌ Fail!"
                hasfail = True
        # Just another display call, for good luck
        state.display(stdscr)
        stdscr.refresh()

    # A nice finished message, a screen update, and an await key to exit
    if not hasfail:
        for ix, line in enumerate(EASTER_EGG.splitlines()):
            stdscr.addstr(curses.LINES // 3 + ix, 20, line, curses.color_pair(3))
    else:
        state.lower_lines.append("Finished all tasks, press q key to exit")
        state.display(stdscr)
    stdscr.refresh()
    # Loop forever until we see a "q" key
    while stdscr.getkey() != "q":
        pass


EASTER_EGG = r"""
+-------------------------------------------------------------+
|     ###########                                             |
|    ##         ##        /------------------------------\    |
|    #  ~~   ~~  #        |   Great work champion, all   |    |
|    #  ()   ()  #        |   code ran!  Now mash that   |    |
|    (     ^     )    /---|   merge button and lets      |    |
|     |         |    /    |       keep moving.           |    |
|     |  \___/  |  -/     |     Press q key to exit...   |    |
|      \       /          \------------------------------/    |
|     /  -----  \                                             |
|  ---  |%\ /%|  ---                                          |
| /     |%%%%%|     \                                         |
|/      |%/ \%|      \                                        |
+-------------------------------------------------------------+
"""


@click.group()
@click.option("--fast/--no-fast", default=False)
def cli(fast):
    ModelFactoryTask.fast = fast


@cli.command("tasks")
@click.argument("task_keys", nargs=-1, type=click.Choice([t.name for t in tasks]))
def task(task_keys):
    curses.wrapper(run_tasks, task_keys)


@cli.command("integrate")
def integration_test():
    task_keys = ["scaleup-2a", "churn", "inlife-ssm_rgb_tipe_a_segment_a"]
    curses.wrapper(run_tasks, task_keys)


@cli.command("fourg")
def fourg_all():
    task_keys = [
        "fourg-SSM_4GScaleUp1_Path2Churn02_SMSApp",
        "fourg-SSM_4GScaleUp2_Path2Churn02_SMSApp",
        "fourg-SSM_4GScaleUp3a_Path2Churn02_SMSApp",
        "fourg-SSM_4GScaleUp3b_Path2Churn08_4GBonus_SMSApp",
        "fourg-SSM_4GScaleUp4a_Path2Churn02_SMSApp",
        "fourg-SSM_4GScaleUp4b_GrabnGo01_SMSApp",
        "fourg-SSM_4GScaleUp5_Path2Churn08_4GBonus_SMSApp",
        "fourg-SSM_4GScaleUp6a_Path2Churn02_SMSApp",
        "fourg-SSM_4GScaleUp6b_GrabnGo01_SMSApp",
        "fourg-SSM_4GScaleUp7a_Path2Churn02_SMSApp",
        "fourg-SSM_4GScaleUp7b_GrabnGo01_SMSApp",
        "fourg-SSM_4GScaleUp8a_GrabnGo01_SMSApp",
        "fourg-SSM_4GScaleUp8b_Path2Churn02_SMSApp",
        "fourg-SSM_4GScaleUp8c_GrabnGo06_SMSApp",
        "fourg-SSM_4GScaleUp8d_VoiceUL_4GB4G_SMSApp",
        "fourg-SSM_4GScaleUp9_GrabnGo01_SMSApp",
        "fourg-SSM_4GScaleUp10_GrabnGo01_SMSApp",
        "fourg-ANMAR_HVC_Path_to_Churn_2a_scaleup",
        "fourg-ANMAR_HVC_Path_to_Churn_8b_scaleup",
    ]
    curses.wrapper(run_tasks, task_keys)


@cli.command("inlife")
def inlife_all():
    task_keys = [
        "inlife-fg_lapser_part1_segment_1_sms",
        "inlife-ssm_hvc_lapser_a_lp5d_apr",
        "inlife-ssm_thrifty_2b",
        "inlife-ssm_thrifty_2d",
        "inlife-ssm_thrifty_3h",
        "inlife-SSM_4GScaleUp4a_Path2Churn02_SMSApp",
        "inlife-SSM_4GScaleUp1_Path2Churn02_SMSApp",
        "inlife-SSM_4GScaleUp6a_Path2Churn02_SMSApp",
        "inlife-SSM_4GScaleUp3b_Path2Churn08_4GBonus_SMSApp",
        "inlife-SSM_4GScaleUp2_Path2Churn02_SMSApp",
        "inlife-SSM_4GScaleUp5_Path2Churn08_4GBonus_SMSApp",
    ]
    curses.wrapper(run_tasks, task_keys)


@cli.command("inlife_high_arpu")
def inlife_high_arpu():
    task_keys = [
        "inlife-AAGM_CMP_THRIFTY-2ND-SEG2_2A",
        "inlife-AAGM_CMP_THRIFTY-2ND-SEG2_2C",
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4H-1",
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4I-1",
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4H_4I-1",
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4A-1",
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4B-1",
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4C-1",
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4A_4B_4C-1",
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4D-1",
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4E-1",
        "inlife-AAGM_CMP_INLIFE-DISCHUNT-SEG4_4D_4E-1",
    ]
    curses.wrapper(run_tasks, task_keys)


@cli.command("fourg_as_inlife")
def fourg_as_inlife_all():
    task_keys = [
        "inlife-SSM_4GScaleUp8b_Path2Churn02_SMSApp",
        "inlife-SSM_4GScaleUp4a_Path2Churn02_SMSApp",
        "inlife-SSM_4GScaleUp1_Path2Churn02_SMSApp",
        "inlife-SSM_4GScaleUp6a_Path2Churn02_SMSApp",
        "inlife-SSM_4GScaleUp3a_Path2Churn02_SMSApp",
        "inlife-SSM_4GScaleUp3b_Path2Churn08_4GBonus_SMSApp",
        "inlife-SSM_4GScaleUp7a_Path2Churn02_SMSApp",
        "inlife-SSM_4GScaleUp2_Path2Churn02_SMSApp",
        "inlife-SSM_4GScaleUp5_Path2Churn08_4GBonus_SMSApp",
        "inlife-multisim4g_USSD_Others_20K",
        "inlife-multisim4g_USSD_4GSD_20K",
        "inlife-multisim4g_Mytsel_4GSD_20K",
        "inlife-multisim4g_Mytsel_Others_10K",
        "inlife-multisim4g_Others_Others_10K",
        "inlife-multisim4g_Mytsel_4GSD_10K",
        "inlife-multisim4g_USSD_4GSD_10K",
        "inlife-multisim4g_USSD_Others_10K",
        "inlife-multisim4g_Mytsel_Others_20K",
        "inlife-multisim4g_Others_4GSD_10K",
    ]
    curses.wrapper(run_tasks, task_keys)


if __name__ == "__main__":
    cli()
