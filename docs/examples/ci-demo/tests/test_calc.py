"""Tests for the ci_demo package."""

import pytest
from ci_demo import add, multiply, divide, factorial


class TestAdd:
    def test_add_positive(self):
        assert add(2, 3) == 5

    def test_add_negative(self):
        assert add(-1, -2) == -3

    def test_add_zero(self):
        assert add(0, 5) == 5
        assert add(5, 0) == 5


class TestMultiply:
    def test_multiply_positive(self):
        assert multiply(4, 5) == 20

    def test_multiply_by_zero(self):
        assert multiply(10, 0) == 0
        assert multiply(0, 10) == 0

    def test_multiply_negative(self):
        assert multiply(-3, 4) == -12
        assert multiply(-3, -4) == 12


class TestDivide:
    def test_divide_normal(self):
        assert divide(10, 2) == 5

    def test_divide_negative(self):
        assert divide(-10, 2) == -5

    def test_divide_by_zero_raises(self):
        with pytest.raises(ZeroDivisionError, match="Cannot divide by zero"):
            divide(1, 0)


class TestFactorial:
    def test_factorial_zero(self):
        assert factorial(0) == 1

    def test_factorial_one(self):
        assert factorial(1) == 1

    def test_factorial_five(self):
        assert factorial(5) == 120

    def test_factorial_negative_raises(self):
        with pytest.raises(ValueError, match="negative"):
            factorial(-1)
