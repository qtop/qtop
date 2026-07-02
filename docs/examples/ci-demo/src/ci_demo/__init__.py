##
## CI Demo — companion project for qtop CI/CD patterns
##
## SPDX-License-Identifier: MIT
##

"""Minimal Python package to exercise CI/CD pipelines."""

__version__ = "0.1.0"


def add(a, b):
    """Add two numbers."""
    return a + b


def multiply(a, b):
    """Multiply two numbers."""
    return a * b


def divide(a, b):
    """Divide two numbers. Raises ZeroDivisionError on b=0."""
    if b == 0:
        raise ZeroDivisionError("Cannot divide by zero")
    return a / b


def factorial(n):
    """Compute factorial of n (n >= 0, integer)."""
    if n < 0:
        raise ValueError("Factorial is not defined for negative numbers")
    if n == 0:
        return 1
    return n * factorial(n - 1)
