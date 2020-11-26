from .. import utils


class ExtrConfig:
    def __init__(self, all_cols):
        self.config = {}
        self.all_cols = all_cols

    def update_with_config_and_cols(self, cols_config, cols):
        for col in cols:
            self.set_col_config(col, cols_config)

    def set_diff_config(self, config, diff_pattern="diff"):
        diff_cols = utils.filter_by_substring(self.all_cols, diff_pattern)
        self.update_with_config_and_cols(config, diff_cols)

    def set_col_config(self, col, col_config):
        self.config[col] = col_config

    def get_config(self):
        return self.config