import re

zag = re.compile(' ?\([^\)]*\)')
resplit = re.compile('/|\n')


def cl(s):
    c = zag.sub('', s).strip()
    return c
