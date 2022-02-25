import libfz as core
import pygdbt
from pygdbt.common import *
from fea_info import *
from feature_generator import win_
import encoder as ENC

cache_uri = None
index_col = -1
bias_slot = -233

def _debug_(data, info = "debug"):
    def graph():
        block = data.Read()
        layers.Output(block, info)
    pygdbt.execute(graph)

def select(src, dst, slots, label=False):
    def graph():
        block = src.Read()
        out   = core.BlockFilter(block, slots, label)
        layers.Sink(out, dst)
    pygdbt.execute(graph, False)

AssignIndex = core.AssignIndex
SplitBySign = core.SplitBySign
FilterBySign = core.FilterBySign
transfer_data = core.transfer_data
gc_memory_size = core.gc_memory_size
SignCountStat = core.SignCountStat
SignCountTable = core.SignCountTable
SignLengthStat = core.SignLengthStat
SignLengthTable = core.SignLengthTable
get_time_interval = core.get_time_interval

class AggConf:
    def __init__(self, name, type, slot, encoder):
        self.name = name
        self.type = type
        self.slot = slot
        self.enc  = encoder
    def conf(self):
        return core.AggConf(self.type, self.slot)
    def __lt__(self, rhs):
        return self.name < rhs.name

def partition_by(data, distribution, lkey, rkey = None, timec = -1):
    if rkey is None:
        rkey = lkey
    return core.partition_by(data, distribution, lkey, rkey, timec)

def left_join(lhs, rhs, lkey, rkey = None):
    if rkey is None:
        rkey = lkey

    lslot = lhs.get_slots(lkey)
    rslot = rhs.get_slots(rkey)
    ldata = pygdbt.Cache(cache_uri)
    rdata = rhs.data

    select(lhs.data, ldata, [index_col] + lslot)
    lpart = partition_by(ldata, lhs.distribution, lslot[0], lslot[0])
    rpart = partition_by(rdata, lhs.distribution, rslot[0], lslot[0])
    core.left_join(lpart, rpart, lslot, rslot, 0, False)
    transfer_data(ldata, lpart, rhs.num_cols() + 1 - len(rslot))

    lpart = partition_by(lhs.data, lhs.distribution, index_col)
    rpart = partition_by(ldata,    lhs.distribution, index_col)
    core.left_join(lpart, rpart, [index_col], [index_col], lhs.offset, True)
    transfer_data(lhs.data, lpart, lhs.num_cols() + rhs.num_cols() - len(rslot))

    relations = set(rkey)
    for f in rhs.info:
        if f.name not in relations:
            name = 'multi_direct(%s,%s)' % (lhs.name, f.feql)
            fea = FeaInfo(name, f.slot + lhs.offset, f.encoder)
            lhs.append_fea(fea)

    lhs.offset += rhs.num_cols()
    ldata.reset()

def last_join(lhs, rhs, lkey, rkey, lt, rt, win):
    SLOG('last_join')
    conf = []
    for f in rhs.info:
        if f.name in rkey:
            continue
        name = "multi_last_value(%s,%s,%s)" % (lhs.name, f.feql, win_(win))
        conf.append(AggConf(name, 0, f.slot, f.encoder))

    lslot = lhs.get_slots(lkey)
    rslot = rhs.get_slots(rkey)
    ltime = lhs.get_slot(lt)
    rtime = rhs.get_slot(rt)
    ldata = pygdbt.Cache(cache_uri)
    rdata = rhs.data

    print('last join')
    printf(' '.join(map(lambda x: x.name, conf)))

    select(lhs.data, ldata, [index_col] + sorted(lslot + [ltime]))
    lpart = partition_by(ldata, lhs.distribution, lslot[0], lslot[0])
    rpart = partition_by(rdata, lhs.distribution, rslot[0], lslot[0])
    core.table_join(lpart, rpart, ldata, lslot, rslot, ltime, rtime, win, map3(lambda x: x.conf(), conf))
    lpart.reset()
    rpart.reset()

    lpart = partition_by(lhs.data, lhs.distribution, index_col)
    rpart = partition_by(ldata,    lhs.distribution, index_col)
    core.left_join(lpart, rpart, [index_col], [index_col], lhs.offset, True)
    # lpart.reset()
    rpart.reset()

    for i, f in enumerate(conf):
        enc = ENC.Num() if isinstance(f.enc, ENC.Label) else f.enc
        fea = FeaInfo(f.name, i + lhs.offset, enc)
        lhs.append_fea(fea)

    transfer_data(lhs.data, lpart, lhs.num_cols())
    lhs.offset += rhs.num_cols()
    ldata.reset()

def table_join(lhs, rhs, lkey, rkey, lt, rt, win, conf):
    SLOG('table_join')
    lslot = lhs.get_slots(lkey)
    rslot = rhs.get_slots(rkey)
    ltime = lhs.get_slot(lt)
    rtime = rhs.get_slot(rt)
    ldata = pygdbt.Cache(cache_uri)
    rdata = rhs.data

    print('table join')
    printf('ops: ', ' '.join(map(lambda x: x.name, conf)))

    select(lhs.data, ldata, [index_col] + sorted(lslot + [ltime]), False)
    lpart = partition_by(ldata, lhs.distribution, lslot[0], lslot[0])
    rpart = partition_by(rdata, lhs.distribution, rslot[0], lslot[0])
    core.table_join(lpart, rpart, ldata, lslot, rslot, ltime, rtime, win, map3(lambda x: x.conf(), conf))
    lpart.reset()
    rpart.reset()

    ret = FZData(rhs.name + ".join_tmp")
    ret.data = ldata
    for i, f in enumerate(conf):
        fea = FeaInfo(f.name, i, f.enc)
        ret.append_fea(fea)
    ret.offset = ret.num_cols()
    return ret

def apply_ops(data, part_keys, time_col, win, conf):
    ret = FZData("apply_ops")
    agg_conf = map3(lambda x: x.conf(), conf)
    ret.data = pygdbt.Cache(cache_uri, "apply_ops.data")
    core.apply_ops(data, ret.data, part_keys, index_col, time_col, agg_conf, win)

    ret.append_fea(FeaInfo("$time", 0, ENC.Ignore()))
    for i, f in enumerate(conf):
        fea = FeaInfo(f.name, i + 1, f.enc)
        ret.append_fea(fea)
    ret.offset = ret.num_cols()
    return ret

# 参数ratio为分配给左边的比例
# 分界点为 lis[lis.size * ratio]，小于分界点的一份，大于的一份，等于的部分随机，概率为本函数的返回值
def _count_(lis, ratio):
    idx = int(len(lis) * ratio) - 1
    tar = lis[idx] if idx >= 0 else lis[0]
    if tar == lis[-1]:
        return ratio, tar
    else:
        return 1.0, tar
        # 当前逻辑为等于分界点的全部归左值，注释部分为按数量概率切
        # a = 0.0
        # b = 0.0
        # for i in range(idx):
        #     if lis[i] == tar:
        #         a += 1
        #         b += 1
        # for l in lis[idx:]:
        #     b += 1
        # return a / b

# ratio为test_ratio
def split_data(data, dist, slot, ratio):
    train = pygdbt.Cache(cache_uri, "train")
    valid = pygdbt.Cache(cache_uri, "valid")
    dist  = dist.pull(slot)
    ratio, cond = _count_(dist, 1.0 - ratio)
    def split() :
        block = data.Read()
        patch = SplitBySign(block, slot, cond, ratio, "old", "new")
        olddt = patch.sub("old")
        newdt = patch.sub("new")
        layers.Sink(olddt, train)
        layers.Sink(newdt, valid)
    pygdbt.execute(split)
    return train, valid

class CombConf:
    def __init__(self, name, type, slots, encoder):
        self.name = name
        self.type = type
        self.slot = slots
        self.enc  = encoder
    def conf(self):
        return core.CombConf(self.type, self.slot)
    def __lt__(self, rhs):
        return self.name < rhs.name

def combine_ops(data, conf):
    ret = FZData("combined")
    ret.data = pygdbt.Cache(cache_uri, "combined")
    def graph():
        block = data.Read()
        comb  = core.Combine(block, map3(lambda x: x.conf(), conf))
        layers.Sink(comb, ret.data)
    pygdbt.execute(graph)

    ret.append_fea(FeaInfo("$time", 0, ENC.Ignore()))
    for i, f in enumerate(conf):
        fea = FeaInfo(f.name, i, f.enc)
        ret.append_fea(fea)
    return ret

def _shuffle_(data, dist):
    part = fztools.partition_by(data, dist, index_col, index_col)
    transfer_data(data, part, 0)

def filter(data, slots):
    cache = pygdbt.Cache(cache_uri)
    select(data, cache, [index_col] + slots, True)
    return cache
