import logging
import re


def search_pattern(pat, filepath):
    matches = []
    reg = re.compile(pat)
    counter = 1
    file = open(filepath, "r", encoding="UTF-8")
    for line in file:
        match = reg.search(line)
        if match:
            matches.append((counter, match.group(0)))
        counter += 1
    return matches


def run_search_unwanted_strings(argv=None):
    pattern_message_pair = [
        # MSISDN numbers is the quoted number that has "628" in the beginning
        # followed by 9-12 digits
        ("[\",']628[0-9]{9,12}[\",']", "Don't commit string that looks like MSISDNs",),
        # Outlet ID is 10-digit quoted string that has "1", "2," up to "6" in the
        # beginning
        ("[\",'][1-6][0-9]{9}[\",']", "Don't commit string that looks like Outlet ID",),
        # NIK has these following structures :
        # 1. 6-digit area code
        # 2. 6-digit birthdate with ddmmyy format, with (dd + 40) if the person
        #    is female. For example, female that born on 121292 will be coded
        #    as 511292
        # 3. 4-digit unique identifier
        (
            "[\",'][0-9]{6}([1256][0-9]|[37][0-1]|[40][1-9])(0[1-9]|1[0-2])[0-9]{6}[\",']",
            "Don't commit string that looks like NIK",
        ),
    ]

    logger = logging.getLogger(__name__)
    allowed = True
    for (pattern, message) in pattern_message_pair:
        results = []
        for filepath in argv:
            try:
                result = search_pattern(pattern, filepath)
                if len(result) > 0:
                    results.append((filepath, result))
            except PermissionError:
                logger.info("File %s couldn't be read, skipped" % filepath)
        if len(results) > 0:
            for (filepath, result) in results:
                log = ["\n" + message + "\n"]
                for (line, string) in result:
                    log.append("Line %s: %s\n" % (line, string))
                file = open(filepath, "a+", encoding="UTF-8")
                file.seek(0)
                file.write("".join(log))
                file.close()
            allowed = False
    return 1 if not allowed else 0


if __name__ == "__main__":
    exit(run_search_unwanted_strings())
