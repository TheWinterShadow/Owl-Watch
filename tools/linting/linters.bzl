"""Linter Bazel rules for Owl-Watch project."""

def _make_lint_check(name, entry_point):
    if entry_point == "black":
        cmd_command = "$(location //tools/linting:runner) {} --config=$(rootpath //:pyproject.toml) --check --diff --line-length 120 $(locations //src:all_py_files) > $@ 2>&1 && echo '{}: No formatting issues' > $@ || (echo '{} formatting issues found:' && $(location //tools/linting:runner) {} --config=$(rootpath //:pyproject.toml) --check --diff --line-length 120 $(locations //src:all_py_files) 2>&1| tee $@ && exit 1)".format(entry_point, name.title(), name.title(), entry_point)
    elif entry_point == "isort":
        cmd_command = "$(location //tools/linting:runner) {} --check-only --diff --profile black --line-length 120 $(locations //src:all_py_files) > $@ 2>&1 && echo '{}: No import issues' > $@ || (echo '{} import sorting issues found:' && $(location //tools/linting:runner) {} --check-only --diff --profile black --line-length 120 $(locations //src:all_py_files) 2>&1 | tee $@ && exit 1)".format(entry_point, name.title(), name.title(), entry_point)
    elif entry_point == "flake8":
        cmd_command = "$(location //tools/linting:runner) {} --max-line-length=120 $(locations //src:all_py_files) > $@ 2>&1 && echo '{}: No issues found' > $@ || (echo '{} issues found:' && $(location //tools/linting:runner) {} --max-line-length=120 $(locations //src:all_py_files) 2>&1 | tee $@ && exit 1)".format(entry_point, name.title(), name.title(), entry_point)
    elif entry_point == "mypy":
        cmd_command = "$(location //tools/linting:runner) {} --config-file=$(rootpath //:mypy_config) $(locations //src:all_py_files) > $@ 2>&1 && echo '{}: No type issues found' > $@ || (echo '{} type issues found:' && $(location //tools/linting:runner) {} --config-file=$(rootpath //:mypy_config) $(locations //src:all_py_files) 2>&1 | tee $@ && exit 1)".format(entry_point, name.title(), name.title(), entry_point)
    else:
        fail("Unsupported linter: {}".format(entry_point))

    if entry_point == "mypy":
        srcs = ["//src:all_py_files", "//:mypy_config"]
    elif entry_point == "black":
        srcs = ["//src:all_py_files", "//:pyproject.toml"]
    else:
        srcs = ["//src:all_py_files"]
    native.genrule(
        name = name + "_check",
        srcs = srcs,
        outs = [name + "_report.txt"],
        cmd = cmd_command,
        tools = ["//tools/linting:runner"],
    )

def black_lint(name = "black"):
    _make_lint_check(name, "black")

def isort_lint(name = "isort"):
    _make_lint_check(name, "isort")

def flake8_lint(name = "flake8"):
    _make_lint_check(name, "flake8")

def mypy_lint(name = "mypy"):
    _make_lint_check(name, "mypy")

def _make_ts_lint_check(name, entry_point):
    if entry_point == "eslint":
        cmd_command = "$(location //tools/linting:ts_runner) {} --ext .ts $(locations //cdk:all_ts_files) > $@ 2>&1 && echo '{}: No linting issues' > $@ || (echo '{} linting issues found:' && $(location //tools/linting:ts_runner) {} --ext .ts $(locations //cdk:all_ts_files) 2>&1 | tee $@ && exit 1)".format(entry_point, name.title(), name.title(), entry_point)
    elif entry_point == "prettier":
        cmd_command = "$(location //tools/linting:ts_runner) {} --check $(locations //cdk:all_ts_files) > $@ 2>&1 && echo '{}: No formatting issues' > $@ || (echo '{} formatting issues found:' && $(location //tools/linting:ts_runner) {} --check $(locations //cdk:all_ts_files) 2>&1 | tee $@ && exit 1)".format(entry_point, name.title(), name.title(), entry_point)
    elif entry_point == "tsc":
        cmd_command = "$(location //tools/linting:ts_runner) {} --noEmit > $@ 2>&1 && echo '{}: No type issues found' > $@ || (echo '{} type issues found:' && $(location //tools/linting:ts_runner) {} --noEmit 2>&1 | tee $@ && exit 1)".format(entry_point, name.title(), name.title(), entry_point)
    else:
        fail("Unsupported TypeScript linter: {}".format(entry_point))

    native.genrule(
        name = name + "_check",
        srcs = ["//cdk:all_ts_files"],
        outs = [name + "_report.txt"],
        cmd = cmd_command,
        tools = ["//tools/linting:ts_runner"],
    )

def eslint_lint(name = "eslint"):
    _make_ts_lint_check(name, "eslint")

def prettier_lint(name = "prettier"):
    _make_ts_lint_check(name, "prettier")

def tsc_lint(name = "tsc"):
    _make_ts_lint_check(name, "tsc")
