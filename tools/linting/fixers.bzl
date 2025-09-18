"""Fixer Bazel rules for Owl-Watch project."""

load("@pip_dev//:requirements.bzl", "requirement")

def _make_lint_fix(name, entry_point):
    if entry_point == "black":
        cmd_command = "$(location //tools/linting:runner) {} $(locations //lock_and_key:all_py_files) && touch $@".format(entry_point)
    elif entry_point == "isort":
        cmd_command = "$(location //tools/linting:runner) {} $(locations //lock_and_key:all_py_files) && touch $@".format(entry_point)
    else:
        fail("Unsupported fixer: {}".format(entry_point))

    native.genrule(
        name = name + "_fix",
        srcs = ["//lock_and_key:all_py_files"],
        outs = [name + "_fix.stamp"],
        cmd = cmd_command,
        tools = ["//tools/linting:runner"],
    )

def black_fix(name = "black"):
    _make_lint_fix(name, "black")

def isort_fix(name = "isort", deps = None):
    _make_lint_fix(name, "isort", deps or [requirement("isort")])

def _make_ts_lint_fix(name, entry_point):
    if entry_point == "eslint":
        cmd_command = "$(location //tools/linting:ts_runner) {} --fix --ext .ts $(locations //cdk:all_ts_files) && touch $@".format(entry_point)
    elif entry_point == "prettier":
        cmd_command = "$(location //tools/linting:ts_runner) {} --write $(locations //cdk:all_ts_files) && touch $@".format(entry_point)
    else:
        fail("Unsupported TypeScript fixer: {}".format(entry_point))

    native.genrule(
        name = name + "_fix",
        srcs = ["//cdk:all_ts_files"],
        outs = [name + "_fix.stamp"],
        cmd = cmd_command,
        tools = ["//tools/linting:ts_runner"],
    )

def eslint_fix(name = "eslint"):
    _make_ts_lint_fix(name, "eslint")

def prettier_fix(name = "prettier"):
    _make_ts_lint_fix(name, "prettier")
