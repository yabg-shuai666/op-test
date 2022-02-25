# -*- coding: utf-8 -*-

from pygdbt.app_common import *
from pygdbt.linear_model import LinearRegression
from pygdbt.feature_extraction import LinearFractal
import fztools

quantileN = 10

def win_to_seconds(w):
    if isinstance(w, int):
        return w
    try:
        w = int(w)
        return w
    except:
        pass

    unit = w[-1]
    w = int(w[:-1])

    if unit == 's':
        w *= 1
    elif unit == 'm':
        w *= 60
    elif unit == 'h':
        w *= 60 * 60
    elif unit == 'd':
        w *= 60 * 60 * 24
    elif unit == 'w':
        w *= 60 * 60 * 24 * 7
    else:
        raise Exception('')
    return w

class DataConf(Configure):
    def __init__(self):
        Configure.__init__(self)
        self.declare('name', "input entity name",
            False, DefaultChecker(), '')
        self.declare('uri', "input entity file uri",
            False, DefaultChecker(), list_config(TrainInputURI))
        self.declare('data_type', "input path data type",
            True, EnumChecker(set(["parquet", "csv", "tsv"])), "parquet")
        self.declare('block_size', "input data block size",
            True, GreaterChecker(0), 100)
        self.declare('is_use_global_shuffle', "input data is use global shuffle",
            True, DefaultChecker(), True)

class AutoConf(Configure):
    def __init__(self):
        Configure.__init__(self)
        self.declare('window_limit', 'default time window up limit',
            True, GreaterChecker(0), 1000)
        self.declare('window_candidates', 'default window candidates, support s/m/h/d/w',
            True, DefaultChecker(), ['2h', '10h', '2d', '7d', '14d', '32d', '64d', '366d'])
        self.declare('groupby_windows', 'temporal feature explore windows',
            True, DefaultChecker(), [])
        self.declare('groupby_feature_limits', 'total explored temporal feature num',
            True, GreaterChecker(-1), 0)
        self.declare('combine_feature_limits', 'total explored combine feature num',
            True, GreaterChecker(-1), 0)
        self.declare('combine_explore_limits', '',
            True, GreaterChecker(0), 300)
        self.declare('rebase_limits', '', True, GreaterChecker(0), 500)
        self.declare('window_delay', 'default window lower bound',
            True, DefaultChecker(), '0s')
        self.declare('partition_target', 'groupby partition target key list, only main table avaliable',
            True, DefaultChecker(), [])
        self.declare('default_window', 'default window(line)',
            True, DefaultChecker(), [])

    def load_config(self, args):
        if not Configure.load_config(self, args):
            return False

        try:
            win_to_seconds(self.window_delay)
        except:
            WARNING('error delay format [%s]' % self.window_delay)
            return False
        for w in self.window_candidates:
            try:
                win_to_seconds(w)
            except:
                WARNING('error window format [%s]' % w)
                return False
        return True

class FZConf(BaseAppConf):
    def __init__(self):
        BaseAppConf.__init__(self)
        self.remove(['init_model', 'input_validation_path', 'validate_model_interval', 'sink_model_interval',
            'model_output_prefix', 'early_stop', 'training_mode', 'mini_batch_size'])
        self.declare('input_path', "input entitys",
            False, DefaultChecker(), list_config(DataConf))
        self.declare('feature_info', "feature info configure json",
            True, DefaultChecker(), '')
        self.declare('feature_info_str', "feature info configure json as yaml string",
            True, DefaultChecker(), '')
        self.declare('lr', "lr configure",
            True, DefaultChecker(), Configure(LinearRegression.Conf))
        self.declare('lfc', "lfc configure",
            True, DefaultChecker(), Configure(LinearFractal.Conf))
        self.declare('test_ratio_accuracy', "sort cut column accuracy",
            True, GreaterChecker(10), 500)
        self.declare('test_ratio', "test ratio in input path",
            True, RangeCheckerOO(0.0, 1.0), 0.2)
        self.declare('selected_op_num', "select fe op num in result",
            True, DefaultChecker(), [6, 18])
        self.declare('multi_table_select_ratio', "multi table join feature select ratio",
            True, GreaterChecker(0), 2)
        self.declare('sample_rows', "block sample row nums in feature_zero score",
            True, DefaultChecker(), 1.5e6)
        self.declare('hotkey_threshold_ratio', "is_hot_key = key_num / tot_ins > hotkey_threshold_ratio, ratio = 1/shard if ratio == 0.0",
            True, RangeCheckerCC(0, 1), 0.1)
        self.declare('task_type', "task type",
            True, EnumChecker(set([ 'binary', 'regression',
                                    'regression_mae', 'regression_mse',
                                    'regression_real_mae', 'regression_real_mse'
                                    ])), 'binary')
        self.declare('disable_calculate', 'disable all cpu bound calculate',
            True, DefaultChecker(), False)
        self.declare('max_iter', 'max training pass num',
            True, GreaterChecker(0), 8)
        self.declare('min_iter', 'max training pass num',
            True, GreaterChecker(0), 4)
        self.declare('auto', 'auto configure method', True, DefaultChecker(), AutoConf())
        self.declare('disable_ops', 'disable ops',
            True, DefaultChecker(), [])

        self.lr.remove(['warm_start'])
        self.label = None
        self.timestamp = None
        self.weight = '__weight__'
        self.hotkey_threshold = None
        self._default_value_()

    def _default_value_(self):
        self.training_pass_num = 3
        self.model_output_path = RawURI()
        self.lfc.binning_bucket_list = '10 100'
        self.lr.alpha = 0.05
        self.lr.beta = 0.0001
        self.lr.max_iter = 4
        self.lr.mini_batch_size = 500
        self.lr.training_mode = 'sync'
        self.lr.penalty = 'l1'

    def load_config(self, args):
        sample_rows = self.sample_rows
        if 'sample_rows' in args:
            if args['sample_rows'] is not None:
                sample_rows = args['sample_rows']
            del args['sample_rows']
        if not BaseAppConf.load_config(self, args):
            return False
        self.selected_op_num = map3(int, self.selected_op_num)

        self.distribution_accuracy = pico_tools.cpu_count() * pico_tools.comm_size() * quantileN
        self.hotkey_threshold = self.hotkey_threshold_ratio * self.distribution_accuracy
        if self.hotkey_threshold < 1.0:
            WARNING('hotkey_threshold_ratio too small for fz to recognize.')
            return False

        if not isinstance(sample_rows, list):
            if self.training_pass_num == 1:
                self.sample_rows = [sample_rows]
            else:
                totl = int(sample_rows)
                half = int(totl / 2)
                diff = int((totl - half) / (self.training_pass_num - 1))
                self.sample_rows = [(half + i*diff) for i in range(self.training_pass_num)]
        else:
            self.sample_rows = sample_rows
        self.sample_rows = map3(float, self.sample_rows)
        self.max_sample_rows = max(self.sample_rows)

        self.lfc.bucket_list = map3(int, self.lfc.binning_bucket_list.split(' '))
        self.lfc.bin_value_type = {
            "same"   : layers.LFCType.SAME,
            "norm"   : layers.LFCType.NORM,
            "origin" : layers.LFCType.ORIGIN
        }[self.lfc.inner_bin_value_type]

        fztools.cache_uri = self.cache_uri

        self.auto1()
        return True

    def auto1(self):
        if 'block_size' not in self.cache_uri.conf:
            bs = int(self.lr.mini_batch_size / (pico_tools.cpu_count() * pico_tools.comm_size())) + 1
            self.cache_uri.conf['block_size'] = bs

    def auto2(self, main):
        line_num = main.data.global_size()
        if self.auto.groupby_feature_limits == 0:
            if line_num < 2e6:
                self.auto.groupby_feature_limits = 5000
            elif line_num < 1e7:
                self.auto.groupby_feature_limits = 1000
            elif line_num < 5e7:
                self.auto.groupby_feature_limits = 500
            else:
                self.auto.groupby_feature_limits = 100
        if self.auto.combine_feature_limits == 0:
            if line_num < 2e6:
                self.auto.combine_feature_limits = 5000
            elif line_num < 1e7:
                self.auto.combine_feature_limits = 1000
            elif line_num < 5e7:
                self.auto.combine_feature_limits = 500
            else:
                self.auto.combine_feature_limits = 100

    def auto_windows(self, table, timestamp, win, delay=None):
        if isinstance(win, list) and len(win) > 0:
            return win
        if delay is None:
            delay = self.auto.window_delay
        interval = fztools.get_time_interval(table, timestamp)

        delay = win_to_seconds(delay)
        cand  = sorted(map3(win_to_seconds, self.auto.window_candidates))
        delta = interval[0] / 1000
        span  = (interval[2] - interval[1]) / 1000

        w = []

        if self.task_type != 'binary':
            i = 0
            while i < len(cand) and (cand[i] <= delta or cand[i] <= delay):
                i += 1
            if i < len(cand):
                w.append('%ds,%d:%ds' % (cand[i], self.auto.window_limit, delay))
            i += 1
            if i < len(cand):
                w.append('%ds,%d:%ds' % (cand[i], self.auto.window_limit, delay))
        else:
            j = len(cand) - 1
            while j > 0 and cand[j] >= span:
                j -= 1
            if j > 0:
                w.append('%ds,%d:%ds' % (cand[j], self.auto.window_limit, delay))
            j -= 1
            if j > 0:
                w.append('%ds,%d:%ds' % (cand[j], self.auto.window_limit, delay))

        if len(w) == 0:
            w.append("10000d,%d:%ds" % (self.auto.window_limit, delay))
        return w if delay > 0 else self.auto.default_window + w
