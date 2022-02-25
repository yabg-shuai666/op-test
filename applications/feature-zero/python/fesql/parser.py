#!/usr/bin/python
# -*- coding: UTF-8 -*-
from feql import parser as feql

Function = 1
Stop = 0
Left_Bucket = 2
Right_Bucket = 3
Comma = 4
Word = 5

LINE_BREAK = '\n'
LEFT_BUCKET = '('
RIGHT_BUCKET = ')'
WHITH_SPACE = ' '
SPLITTER = ','
DOT = '.'


class Token:
    def __init__(self, name, row, col, symbol):
        self.name = name
        self.row = row
        self.col = col
        self.symbol = symbol
        self.uuid = None


class Scanner():
    def scan(self, input, line_number):
        buf = ""
        col = 0
        for c in input:
            col += 1
            if c == feql.WHITH_SPACE or c == feql.LINE_BREAK:
                continue
            if c == feql.LEFT_BUCKET:
                if buf:
                    t = Token(buf, line_number, col - 1, Function)
                    buf = ""
                    yield t
                yield Token(feql.LEFT_BUCKET, line_number, col, Left_Bucket)
            elif c == feql.RIGHT_BUCKET:
                if buf:
                    t = Token(buf, line_number, col - 1, Function)
                    buf = ""
                    yield t
                yield Token(feql.LEFT_BUCKET, line_number, col, Right_Bucket)
            elif c == feql.SPLITTER:
                if buf:
                    t = Token(buf, line_number, col - 1, Function)
                    buf = ""
                    yield t
                yield Token(feql.SPLITTER, line_number, col, Comma)
            else:
                buf += c


class Regex:
    def __init__(self):
        self.stop_words = []
        self.tokens = []

    def parseToken(self, input):
        '''
        根据停止词，切割字符串，返回token列表
        :return:
        '''
        buf = ''
        line = 0
        col = 0
        for c in input:
            col += 1
            if c == LINE_BREAK:
                line += 1
                continue
            if c in self.stop_words:
                if buf != '':
                    t = Token(buf, line, col - 1, Word)
                    buf = ''
                    self.tokens.append(t)
                    yield t
            else:
                buf += c

    def replaceKeys(self, input, tokens):
        '''
        识别关键词，然后加上单引号
        :param input:
        :param keys:
        :return:
        '''
        list_str = list(input)
        for token in reversed(tokens):
            list_str.insert(token.col, '`')
            list_str.insert(token.col - len(token.name), '`')
        return "".join(list_str)
