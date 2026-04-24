"""
Build script for all C++ extensions.

Usage:
    conda run -n backtest python setup.py build_ext --inplace

After running, .so files will appear next to this file and can be imported directly.
"""
from setuptools import setup, Extension
import pybind11
import os
import json
import sys

# Generate compile_commands.json for clangd/IDE
class CompileCommandsGenerator:
    def __init__(self):
        self.commands = []

    def add_extension(self, ext):
        include_dirs = ext.include_dirs + [pybind11.get_include()]
        compile_args = ext.extra_compile_args if hasattr(ext, 'extra_compile_args') else []
        
        # Add standard library paths for clangd/IDE
        std_includes = [
            '/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include',
            '/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/c++/v1',
        ]
        include_dirs = include_dirs + std_includes
        
        for source in ext.sources:
            cmd = {
                "directory": os.path.abspath(os.getcwd()),
                "command": f"clang++ {' '.join(compile_args)} {' '.join(f'-I{d}' for d in include_dirs)} -c {source}",
                "file": source
            }
            self.commands.append(cmd)

    def write(self, filename="compile_commands.json"):
        with open(filename, 'w') as f:
            json.dump(self.commands, f, indent=2)


def make_ext(name, sources):
    """Helper to create a pybind11 Extension with correct flags."""
    return Extension(
        name=name,
        sources=sources,
        include_dirs=[pybind11.get_include()],
        language='c++',
        extra_compile_args=[
            '-std=c++17',   # Use C++17 standard
            '-O3',          # Maximum optimisation
            '-fvisibility=hidden',  # Required by pybind11
        ],
    )


# Define extensions first
ext_modules = [
    make_ext('hello_ext', ['cpp/hello_ext.cpp']),
    make_ext('market_rule_ext', ['cpp/market_rule_ext.cpp']),
    make_ext('indicators_ext',  ['cpp/indicators_ext.cpp']),
    make_ext('portfolio_ext',   ['cpp/portfolio_ext.cpp']),
]

# Generate compile_commands.json
cmd_gen = CompileCommandsGenerator()
for ext in ext_modules:
    cmd_gen.add_extension(ext)
cmd_gen.write()

setup(
    name='backtest_cpp_ext',
    ext_modules=ext_modules,
)
