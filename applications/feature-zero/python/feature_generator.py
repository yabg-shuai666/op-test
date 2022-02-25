from pygdbt.common import *
import fztools
import encoder as ENC

class FeaCtx:
    def __init__(self, fz):
        self.fz = fz
        self.sign_len = fztools.SignLengthTable()
        self.sign_cnt = fztools.SignCountTable()
        self.distribution = tables.QuantileIntTable(fz.conf.distribution_accuracy, False)
        self.score = None
        self.useless_score = None

    def _key_func(self, keys):
        score = map3(lambda x: self.score[x.slot], keys)
        return sum(score) / len(score)

    def set_score(self, score, slots):
        v_map = {}
        for i, v in enumerate(sorted(score.values())):
            v_map[v] = float(i) / len(score)
        s_map = {}
        for k in slots:
            if k in score:
                s_map[k] = v_map[score[k]]
            else:
                s_map[k] = 0
                score[k] = 0

        self.raw_score = score
        self.score = s_map
        self.useless_score = v_map[score[-2]]

    def is_useless(self, f):
        return self.useless_score == self.score[f.slot] and not isinstance(f.encoder, ENC.Label)

    def prepare(self):
        self.partition_keys = []
        self.cur_partition_keys = []

        for f in self.fz.main.info:
            print(f.name, ':', self.score[f.slot], ':', self.raw_score[f.slot], 'useless' if self.is_useless(f) else '')

        info = self.fz.multi_ctx.info
        conf = info['entity_detail'][info['target_entity']]['features']
        info = self.fz.main.info[:len(conf)]

        if len(self.fz.conf.auto.partition_target) > 0:
            name2f = {}
            for f in info:
                name2f[f.name] = f
            for target in self.fz.conf.auto.partition_target:
                if isinstance(target, list):
                    self.cur_partition_keys.append(map3(lambda x: name2f[x], target))
                else:
                    self.cur_partition_keys.append([name2f[target]])
            self.cur_partition_keys.sort(key = self._key_func)

        else:
            for f in info:
                if not ENC.is_single_category(f.encoder):
                    continue
                cnt = self.sign_cnt.pull(f.slot)
                if cnt == pico_tools.comm_size() or cnt == self.fz.main.data.global_size():
                    continue

                cnt = 0
                vis = {}
                dis = self.distribution.pull(f.slot)
                for sign in dis:
                    if sign in vis:
                        vis[sign] += 1
                    else:
                        vis[sign] = 1
                for sign, v in vis.items():
                    cnt = max(cnt, v)
                if cnt > self.fz.conf.hotkey_threshold or self.is_useless(f):
                    continue

                self.partition_keys.append([f])

            self.cur_partition_keys = sorted(self.partition_keys, key = self._key_func)

        self.raw_feas = info
        self.all_feas = self.fz.main.info
        printf(map3(lambda x: (map3(lambda y: y.name, x), self._key_func(x)), self.cur_partition_keys))

    def pop_partition_target(self):
        if len(self.cur_partition_keys) == 0:
            return None

        ret = self.cur_partition_keys[-1]
        del self.cur_partition_keys[-1]
        return ret

    def combinable(self, f1, f2):
        if not ENC.is_category(f1.encoder) or not ENC.is_category(f2.encoder):
            return False
        s1 = self.sign_len.pull(f1.slot)
        s2 = self.sign_len.pull(f2.slot)
        return 0 < s1 * s2 and s1 * s2 <= 20

def win_(w):
    w = str(w).split(':')
    high = w[0]
    usel = high.isdigit()
    if len(w) == 2:
        low = w[1]
    elif high.isdigit():
        low = '0'
    else:
        low = '0s'

    if not usel:
        high = high.split(',')
        if len(high) == 1:
            lim = str(0x7fffffff)
        else:
            lim = high[1]
        high = high[0]
        return "%s:%s:%s" % (high, lim, low)
    else:
        return "%s:%s" % (high, low)
