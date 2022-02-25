#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pygdbt
from pygdbt.common import layers
class IdentityFunction:
    def get_val(self, x):
        return x
    def get_inverse_val(self, x):
        return x

class SigmoidFunction:
    def get_val(self, x):
        return layers.Activation(x, layers.ActType.SIGMOID)
    def get_inverse_val(self, x):
        return None

class Log1pFunction:
    def get_val(self, x):
        return layers.ScaleLog(x)
    def get_inverse_val(self, x):
        return layers.ScaleExp(x)

