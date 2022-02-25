def _define_encoder(F):
    ret = []
    ret += [ 'class %s:' % F ]
    ret += [ '    def __str__(self):' ]
    ret += [ '        return "%s"' % F ]
    return '\n'.join(ret)

exec(_define_encoder('Ignore'))
exec(_define_encoder('Label'))
exec(_define_encoder('InstanceId'))
exec(_define_encoder('String'))
# exec(_define_encoder('ListString'))
exec(_define_encoder('Num'))
exec(_define_encoder('DiscNum'))
exec(_define_encoder('StringDate'))
exec(_define_encoder('Timestamp'))

class KvStringNum:
    def __init__(self, sep = ',', kv_sep = ':'):
        self.sep = sep
        self.kv_sep = kv_sep

    def __str__(self):
        return 'KvStringNum("%s","%s")' % (self.kv_sep, self.sep)

# class KvNumNum:
#     def __init__(self, sep = ',', kv_sep = ':'):
#         self.sep = sep
#         self.kv_sep = kv_sep

#     def __str__(self):
#         return 'KvNumNum("%s","%s")' % (self.kv_sep, self.sep)

class KvString:
    def __init__(self, sep = ',', kv_sep = ':'):
        self.sep = sep
        self.kv_sep = kv_sep

    def __str__(self):
        return 'KvString("%s","%s")' % (self.kv_sep, self.sep)

class SepString:
    def __init__(self, sep = ','):
        self.sep = sep

    def __str__(self):
        return 'SepString("%s")' % self.sep

# class SepNum:
#     def __init__(self, sep = ','):
#         self.sep = sep

#     def __str__(self):
#         return 'SepNum("%s")' % self.sep

def is_numeric(x):
    for t in [Num, KvStringNum, Label]:
        if isinstance(x, t):
            return True
    return False

def is_category(x):
    for t in [String, DiscNum, KvString, SepString]:
        if isinstance(x, t):
            return True
    return False

def is_time(x):
    for t in [Timestamp, StringDate]:
        if isinstance(x, t):
            return True
    return False

def is_single_category(x):
    return isinstance(x, String) or isinstance(x, DiscNum)

def is_multi_category(x):
    return isinstance(x, KvString) or isinstance(x, SepString)

def is_multi(x):
    for t in [SepString, KvString, KvStringNum]:
        if isinstance(x, t):
            return True
    return False
