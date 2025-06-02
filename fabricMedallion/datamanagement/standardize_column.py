# Python imports
import re

def standardize_column(
    c: str,
    pattern: str = r"(?<=[a-z])(?=[A-Z])|[.,;{}/\(\)\n\t=]",
    prefixes: str | list = list()
) -> str:
    """
    Standardizes a given column according to a given regex pattern.
    The default pattern takes uppercase letters, and periods as splitters.
    It drops periods, but keeps uppercase letters
    """
    if isinstance(prefixes, str):
        prefixes = [prefixes]

    for prefix in prefixes:
        c = c.removeprefix(prefix)
    c = re.split(pattern, c)
    c = "_".join(c)
    c = c.lower().replace(" ", "_") # FIXME: the pattern is not capturing blank spaces correctly
    # c = ''.join(
    #         chr for chr in normalize('NFD', c) if not combining(chr)
    #         )

    return c