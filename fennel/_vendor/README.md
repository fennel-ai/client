# Overview
The core idea behind vendoring the dependencies of the client is to ensure that the 
users of the client do not run into python version conflicts with the dependencies already
installed on their system.

This is inspired from how vendoring works in [pip](https://github.com/pypa/pip) and uses 
the tool backing it [vendoring](https://pypi.org/project/vendoring/).

Vendoring is structured as follows:
1. Vendored dependencies are stored in the `fennel/_vendor` directory.
2. Dependencies (and their dependencies) to vendor are listed in `fennel/_vendor/vendor.txt`.
3. Vendoring a dependency could require patching a manual change. These patches are stored in
`fennel/tools/vendor/patches`.
4. Tool `vendoring` is configured in pyproject.toml.

## How to Vendor a dependency
To vendor a dependency, add it to `fennel/_vendor/vendor.txt`. Pick the version of the dependency,
which needs to be vendored. If this package has any dependencies, it will need to be vendored
as well. You will need to choose a version for this as well. Add it as a nested entry for the
dependency in the `vendor.txt` file.

e.g.

```
foo_package==x.y.z
    bar_package==a.b.c
```

Once this is done, run `vendoring sync . -v`.

### Creating a patch
Vendoring a dependency may require a patch. This is because `vendoring sync` downloads the package,
applies transformations to replace any imports of the form `from foo import bar` where `foo` is the
dependency being vendored to `from fennel._vendor.foo import bar`. However, few dependencies may have
absolute imports (of the form `import foo` and this is later used as `foo.bar`). In these cases, `vendoring`
tool fails and requires us to manually patch this change. This can be done as follows:
1. Create a dummy commit at this stage.
2. Edit the file where `vendoring` failed to replace the absolute import with the vendored import.
3. Use `git diff` to create a patch. e.g. `git diff ... >> fennel/tools/vendor/patches/foo.patch`.
4. Run vendoring again. It is possible that this fails, repeat the above steps until vendoring succeeds.

## Need to be careful with vendoring
1. If a dependency is vendored, rest of the code should use the locally vendored dependency. It is possible
that the dependency is already installed on the system and the code is not importing the vendored dependency.
It would not fail/complain and continue to use the system installed dependency. This could have different
versions runnings and lead to unexpected behavior/bugs.
2. A vendored dependnecy should never be edited directly. If a change is required, it should be done via
a patch.
3. See - https://github.com/pypa/pip/blob/main/src/pip/_vendor/README.rst, which also lists some interesting
cases (not applicable to us right now e.g. security or debundling).