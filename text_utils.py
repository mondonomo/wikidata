import re

zag = re.compile(' ?\([^\)]*\)')
resplit = re.compile('/|\n')
retok = re.compile("[\u200B\u202F\u205F\u3000\s・]+")


def cl(s):
    c = zag.sub('', s).strip()
    return c


if __name__ == '__main__':
    print(retok.split('ジョージ・ワシントン'))