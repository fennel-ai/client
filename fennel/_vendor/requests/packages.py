import sys

for package in ("urllib3", "idna", "chardet"):
    vendored_package = f"fennel._vendor.{package}"
    locals()[package] = __import__(vendored_package)
    # This traversal is apparently necessary such that the identities are
    # preserved (requests.packages.urllib3.* is urllib3.*)
    for mod in list(sys.modules):
        if mod == vendored_package or mod.startswith(f"{vendored_package}."):
            unprefixed_mod = mod[len("fennel._vendor."):]
            sys.modules[f"fennel._vendor.requests.packages.{unprefixed_mod}"] = sys.modules[mod]
