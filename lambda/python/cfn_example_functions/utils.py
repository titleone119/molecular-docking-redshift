### utils
def isnan(x):
    return x != x


def has_value(as_dict,key):
    if key not in as_dict:
        return False;
    tmp = as_dict[key]
    if isnan(tmp):
        return False
    return True
