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
try:
    from .timeout_utils import get_timeout_seconds, pro_call_with_timeout
except Exception:
    try:
        from timeout_utils import get_timeout_seconds, pro_call_with_timeout
    except Exception:
        import sys, os
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from timeout_utils import get_timeout_seconds, pro_call_with_timeout

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
        df = df.sort_values(by="cal_date").reset_index(drop=True)
        return df
    except Exception:
        # Fallback: 以工作日近似交易日（可能与法定节假日不符，但避免中断）
        rng = pandas.bdate_range(
            start=datetime.datetime.strptime(start_date, '%Y%m%d'),
            end=datetime.datetime.strptime(end_date, '%Y%m%d')
        )
        cal_df = pandas.DataFrame({"cal_date": rng.strftime('%Y%m%d')})
        return cal_df

index_list = ['399300.SZ', '000905.SH', '000300.SH', '000906.SH', '000852.SH', '000985.SH']

def _ts_code_to_ak_index_symbol(ts_code: str) -> str:
    code, exch = ts_code.split('.')
    prefix = 'sh' if exch == 'SH' else 'sz'
    return f"{prefix}{code}"

def _fetch_index_via_ak(ts_code: str, start_date: str, end_date: str) -> Optional[pandas.DataFrame]:
    if ak is None:
        return None
    try:
        ak_sym = _ts_code_to_ak_index_symbol(ts_code)
        df = ak.stock_zh_index_daily(symbol=ak_sym)
        if df is None or df.empty:
            return None
        if 'date' in df.columns:
            df = df.rename(columns={'date': 'trade_date'})
        df['trade_date'] = pandas.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
        df = df[(df['trade_date'] >= start_date) & (df['trade_date'] <= end_date)]
        if df.empty:
            return None
        if 'vol' not in df.columns and 'volume' in df.columns:
            df['vol'] = df['volume']
        df['ts_code'] = ts_code
        if 'adjclose' not in df.columns and 'close' in df.columns:
            df['adjclose'] = df['close']
        df['tradedate'] = df['trade_date']
        df['symbol'] = df['ts_code']
        df['volume'] = df.get('vol', df.get('volume'))
        return df
    except Exception:
        return None

def _fetch_index_via_yahoo(ts_code: str, start_date: str, end_date: str) -> Optional[pandas.DataFrame]:
    if YqTicker is None:
        return None
    try:
        code, exch = ts_code.split('.')
        y_ex = 'SS' if exch == 'SH' else 'SZ'
        y_sym = f"{code}.{y_ex}"
        tk = YqTicker(y_sym)
        hist = tk.history(start=datetime.datetime.strptime(start_date, '%Y%m%d'),
                          end=datetime.datetime.strptime(end_date, '%Y%m%d') + datetime.timedelta(days=1),
                          interval='1d')
        if hist is None or len(hist) == 0:
            return None
        if isinstance(hist.index, pandas.MultiIndex):
            hist = hist.reset_index()
        if 'date' in hist.columns:
            hist = hist.rename(columns={'date': 'trade_date'})
        if 'volume' in hist.columns and 'vol' not in hist.columns:
            hist = hist.rename(columns={'volume': 'vol'})
        hist['trade_date'] = pandas.to_datetime(hist['trade_date']).dt.strftime('%Y%m%d')
        hist['ts_code'] = ts_code
        if 'adjclose' not in hist.columns and 'close' in hist.columns:
            hist['adjclose'] = hist['close']
        hist['tradedate'] = hist['trade_date']
        hist['symbol'] = hist['ts_code']
        hist['volume'] = hist.get('vol')
        return hist
    except Exception:
        return None

def dump_index_data(start_date=None, end_date=None, skip_exists=True):
    now_dt = datetime.datetime.now()
    if start_date is None:
        start_date = (now_dt - datetime.timedelta(days=14)).strftime('%Y%m%d')
    if end_date is None:
        end_date = now_dt.strftime('%Y%m%d')
    trade_date_df = get_trade_cal(start_date, end_date)

    if not os.path.exists(f"{file_path}/index/"):
        os.makedirs(f"{file_path}/index/")
    
    for index_name in index_list:
        print(f"Processing {index_name}")
        filename = f'{file_path}/index/{index_name}.csv'
        result_df_list = []
        for time_slice in range(int(len(trade_date_df)/4000) + 1):
            start_date = trade_date_df["cal_date"][time_slice * 4000]
            end_index = min((time_slice+1) * 4000 - 1, len(trade_date_df) - 1)
            end_date = trade_date_df["cal_date"][end_index]
            try:
                df = pro_call_with_timeout(
                    pro,
                    'index_daily',
                    get_timeout_seconds(),
                    ts_code=index_name,
                    start_date=start_date,
                    end_date=end_date
                )
                if df is not None and not df.empty:
                    result_df_list.append(df)
                    continue
            except Exception as e:
                print("[WARN] Tushare index_daily failed, fallback:", e)
            df_ak = _fetch_index_via_ak(index_name, start_date, end_date)
            if df_ak is not None and not df_ak.empty:
                result_df_list.append(df_ak)
                continue
            df_y = _fetch_index_via_yahoo(index_name, start_date, end_date)
            if df_y is not None and not df_y.empty:
                result_df_list.append(df_y)
        if len(result_df_list) == 0:
            continue
        result_df = pandas.concat(result_df_list, ignore_index=True)
        if 'trade_date' not in result_df.columns and 'tradedate' in result_df.columns:
            result_df['trade_date'] = result_df['tradedate']
        if 'vol' not in result_df.columns and 'volume' in result_df.columns:
            result_df['vol'] = result_df['volume']
        if 'ts_code' not in result_df.columns:
            result_df['ts_code'] = index_name
        if 'adjclose' not in result_df.columns and 'close' in result_df.columns:
            result_df['adjclose'] = result_df['close']
        result_df["tradedate"] = result_df["trade_date"]
        result_df["volume"] = result_df["vol"]
        result_df["symbol"] = result_df["ts_code"]
        result_df = result_df.drop_duplicates(subset=['tradedate', 'symbol']).sort_values(['tradedate', 'symbol'])
        result_df.to_csv(filename, index=False)

if __name__ == '__main__':
    fire.Fire(dump_index_data)
