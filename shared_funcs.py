
def celsius_to_fahrenheit(num):
    result = (float(num) * 1.8) + 32
    return round(result, 2)


def millibar_to_hg(num):
    result = float(num) * 0.02953
    return round(result, 2)


# mps is short for Meters Per Second.
def mps_to_mph(num):
    result = float(num) * 2.23694
    return round(result, 2)


def meters_to_miles(num):
    result = float(num) * 0.000621371
    return round(result, 2)


def meters_to_feet(num):
    result = float(num) * 3.28084
    return round(result, 2)


def millimeters_to_inches(num):
    result = float(num) * 0.0393701
    return round(result, 2)


def logger(file_name):
    import logging

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    f_format = logging.Formatter('%(levelname)s:%(name)s:%(asctime)s:%(message)s')
    f_handler = logging.FileHandler(file_name)
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)

    return logger
