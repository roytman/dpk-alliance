import os


def compute_data_location(loc: str) -> str:
    """
    Compute absolute data location
    :param loc: partial location
    :return: absolute location
    """
    location = os.path.dirname(__file__)
    dirs = location.split("src")
    sp = dirs[0] + "src" + dirs[1]
    return sp + loc


if __name__ == "__main__":
    res = compute_data_location("test-data/noop")
    print(res)
