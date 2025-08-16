import tushare as ts
import os
import datetime
import pandas
import fire
import time
from typing import Optional

try:
    import akshare as ak
except Exception:
    ak = None

try:
    from yahooquery import Ticker as YqTicker
except Exception:
    YqTicker = None
from .timeout_utils import get_timeout_seconds, pro_call_with_timeout

ts.set_token(os.environ["TUSHARE"])
pro=ts.pro_api()
file_path = os.path.dirname(os.path.realpath(__file__))


def get_trade_cal(start_date, end_date):
    try:
        df = pro_call_with_timeout(
            pro,
            'trade_cal',
            get_timeout_seconds(),
            exchange='SSE', is_open='1', start_date=start_date, end_date=end_date, fields='cal_date'
        )
        return df
    except Exception:
        rng = pandas.bdate_range(
            start=datetime.datetime.strptime(start_date, '%Y%m%d'),
            end=datetime.datetime.strptime(end_date, '%Y%m%d')
        )
        cal_df = pandas.DataFrame({"cal_date": rng.strftime('%Y%m%d')})
        return cal_df

def get_daily(trade_date=''):
    for _ in range(3):
        try:
            price_df = pro_call_with_timeout(pro, 'daily', get_timeout_seconds(), trade_date=trade_date)
            adj_factor = pro_call_with_timeout(pro, 'adj_factor', get_timeout_seconds(), trade_date=trade_date)
            df = pandas.merge(price_df, adj_factor, on="ts_code", how="inner")
            df["adj_close"] = df["close"] * df["adj_factor"]
            return df
        except Exception as e:
            print(e)
            time.sleep(1)
    # 回退到 AKShare
    try:
        if ak is not None:
            df_ak = ak.stock_zh_a_daily(symbol="all", adjust="")
            # ak 接口可能返回多表，统一整理
            if isinstance(df_ak, pandas.DataFrame) and not df_ak.empty:
                df_ak = df_ak.copy()
                # 过滤指定交易日
                if 'date' in df_ak.columns:
                    df_ak['trade_date'] = pandas.to_datetime(df_ak['date']).dt.strftime('%Y%m%d')
                if 'trade_date' not in df_ak.columns and '日期' in df_ak.columns:
                    df_ak['trade_date'] = pandas.to_datetime(df_ak['日期']).dt.strftime('%Y%m%d')
                df_ak = df_ak[df_ak['trade_date'] == trade_date]
                # 映射代码列
                for code_col in ['ts_code','ts_code_full','code','代码','symbol']:
                    if code_col in df_ak.columns:
                        df_ak['ts_code'] = df_ak[code_col]
                        break
                # 适配列
                rename_map = {
                    'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close',
                    'vol': 'vol', 'volume': 'vol', '成交量': 'vol'
                }
                for c_from, c_to in list(rename_map.items()):
                    if c_from in df_ak.columns and c_to not in df_ak.columns:
                        df_ak[c_to] = df_ak[c_from]
                # 估算复权收盘（缺少复权因子时以收盘替代）
                df_ak['adj_close'] = df_ak.get('close')
                if 'ts_code' in df_ak.columns and 'open' in df_ak.columns:
                    return df_ak[['ts_code','trade_date','open','high','low','close','vol','adj_close']].rename(columns={'trade_date':'trade_date'})
    except Exception:
        pass
    # 最后兜底：Yahoo 全量（逐只股票）
    try:
        if YqTicker is not None:
            # 简化：返回 None，此脚本多为文件产出示例；全量兜底在 update_a_stock_eod_price_to_latest.py 中实现
            return None
    except Exception:
        return None

def dump_astock_data(start_date, end_date, skip_exists=True):
    trade_date_df = get_trade_cal(start_date, end_date)
    for row in trade_date_df.values.tolist():
        trade_date = row[0]
        filename = f'{file_path}/astock_daily/{trade_date}.csv'
        print(filename)
        if skip_exists and os.path.isfile(filename):
            continue
        data = get_daily(trade_date)
        if data is None:
            continue
        if data.empty:
            continue
        data.to_csv(filename, index=False)

if __name__ == '__main__':
    fire.Fire(dump_astock_data)
