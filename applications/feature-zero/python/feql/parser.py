#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
#
"""

"""

LINE_BREAK='\n'
LEFT_BUCKET='('
RIGHT_BUCKET=')'
WHITH_SPACE=' '
SPLITTER = ','

class Token(object):
    def __init__(self, name, ty):
        self.name = name
        self.ty = ty

class FzScanner(object):

    def scan(self, input):
        buf = "" 
        for c in input:
            if c == WHITH_SPACE or c == LINE_BREAK:
                continue
            if c == LEFT_BUCKET:
                if buf:
                    t = Token(buf, 5)
                    buf = ""
                    yield t
                yield Token(LEFT_BUCKET, 2)
            elif c == RIGHT_BUCKET:
                if buf:
                    t = Token(buf, 5)
                    buf = ""
                    yield t
                yield Token(RIGHT_BUCKET, 3)
            elif c == SPLITTER:
                if buf:
                    t = Token(buf, 5)
                    buf = ""
                    yield t
                yield Token(SPLITTER, 4)
            else:
                buf += c

class VarNode(object):
    def __init__(self, name):
        self.name = name
        self.type = "var"
    
    def get_id(self):
        return self.name

class FnNode(object):
    def __init__(self):
        self.name = None 
        self.args = []
        self.id_ = None
        self.type = "function"
        self.is_gen_feature_ = True

    def add_arg(self, var_node):
        self.args.append(var_node)

    def set_name(self, name):
        self.name = name

    def get_id(self):
        if self.id_:
            return self.id_
        self.id_ = self.name
        for arg in self.args:
            self.id_ += arg.get_id()
        return self.id_
    
    def disable_gen_feature(self):
        self.is_gen_feature_ = False

    def is_gen_feature(self):
        return self.is_gen_feature_
class FzParser(object):

    def parse(self, input):
        scanner = FzScanner()
        tree = []
        buf = []
        for t in scanner.scan(input):
            if t.ty == 5:
                vn = VarNode(t.name)
                buf.append(vn)
            elif t.ty == 2:
                fn = FnNode()
                fn.set_name(buf[0].name)
                if tree:
                    tree[-1].add_arg(fn)
                tree.append(fn)
                buf = []
            elif t.ty == 4:
                if buf:
                    tree[-1].add_arg(buf[0])
                buf = []
            elif t.ty == 3:
                if buf:
                    tree[-1].add_arg(buf[0])
                if len(tree) > 1:
                    tree.pop()
                buf = []
        if len(tree) > 0:
            return tree[0]
        return None






