import tushare as ts
import os
import datetime
import pandas
import fire
import time
from sqlalchemy import create_engine
import pymysql
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

def get_daily(trade_date):
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
    # 回退: AKShare（质量高于 Yahoo）。按 ts_code 列表逐只拉取，保持列名契合后续 rename 映射
    try:
        if ak is not None:
            sqlEngine_local = create_engine('mysql+pymysql://root:@127.0.0.1/investment_data', pool_recycle=3600)
            dbConn_local = sqlEngine_local.raw_connection()
            ts_list_df = pandas.read_sql("select ts_code from ts_a_stock_list", dbConn_local)
            dbConn_local.close()
            sqlEngine_local.dispose()

            def _ts_to_ak_sym(ts_code: str) -> str:
                code, exch = ts_code.split('.')
                prefix = 'sh' if exch == 'SH' else 'sz'
                return f"{prefix}{code}"

            rows = []
            for ts_code in ts_list_df['ts_code'].tolist():
                try:
                    ak_sym = _ts_to_ak_sym(ts_code)
                    df_hist = ak.stock_zh_a_hist(symbol=ak_sym, period="daily", start_date=trade_date, end_date=trade_date, adjust="")
                    if df_hist is None or df_hist.empty:
                        continue
                    # 统一列
                    if '日期' in df_hist.columns:
                        df_hist['trade_date_x'] = pandas.to_datetime(df_hist['日期']).dt.strftime('%Y%m%d')
                    elif 'date' in df_hist.columns:
                        df_hist['trade_date_x'] = pandas.to_datetime(df_hist['date']).dt.strftime('%Y%m%d')
                    else:
                        continue
                    if '开盘' in df_hist.columns:
                        df_hist['open'] = df_hist['开盘']
                    if '收盘' in df_hist.columns:
                        df_hist['close'] = df_hist['收盘']
                    if '最高' in df_hist.columns:
                        df_hist['high'] = df_hist['最高']
                    if '最低' in df_hist.columns:
                        df_hist['low'] = df_hist['最低']
                    if '成交量' in df_hist.columns and 'vol' not in df_hist.columns:
                        df_hist['vol'] = df_hist['成交量']
                    if '成交额' in df_hist.columns and 'amount' not in df_hist.columns:
                        df_hist['amount'] = df_hist['成交额']
                    df_hist['ts_code'] = ts_code
                    df_hist['adj_close'] = df_hist.get('close')
                    keep_cols = ['ts_code','trade_date_x','open','high','low','close','vol','amount','adj_close']
                    row = df_hist[keep_cols].iloc[[0]]
                    rows.append(row)
                except Exception:
                    continue
            if len(rows) > 0:
                return pandas.concat(rows, ignore_index=True)
    except Exception:
        pass
    # 最后兜底：Yahoo 全量补齐（逐只股票，仅当 AKShare 不可用时），确保不中断
    try:
        if YqTicker is not None:
            sqlEngine_local = create_engine('mysql+pymysql://root:@127.0.0.1/investment_data', pool_recycle=3600)
            dbConn_local = sqlEngine_local.raw_connection()
            ts_list_df = pandas.read_sql("select ts_code from ts_a_stock_list", dbConn_local)
            dbConn_local.close()
            sqlEngine_local.dispose()

            def _ts_to_yahoo_sym(ts_code: str) -> str:
                code, exch = ts_code.split('.')
                y_ex = 'SS' if exch == 'SH' else 'SZ'
                return f"{code}.{y_ex}"

            # 分批请求，降低失败风险
            ts_list = ts_list_df['ts_code'].tolist()
            batch_size = 200
            rows = []
            for i in range(0, len(ts_list), batch_size):
                sub_ts = ts_list[i:i+batch_size]
                y_syms = [_ts_to_yahoo_sym(x) for x in sub_ts]
                y2ts = {y: t for y, t in zip(y_syms, sub_ts)}
                try:
                    tk = YqTicker(y_syms)
                    start_dt = datetime.datetime.strptime(trade_date, '%Y%m%d')
                    end_dt = start_dt + datetime.timedelta(days=1)
                    hist = tk.history(start=start_dt, end=end_dt, interval='1d')
                    if hist is None or len(hist) == 0:
                        continue
                    if isinstance(hist.index, pandas.MultiIndex):
                        hist = hist.reset_index()
                    # 统一列
                    if 'date' in hist.columns:
                        hist['trade_date_x'] = pandas.to_datetime(hist['date']).dt.strftime('%Y%m%d')
                    elif 'asOfDate' in hist.columns:
                        hist['trade_date_x'] = pandas.to_datetime(hist['asOfDate']).dt.strftime('%Y%m%d')
                    else:
                        continue
                    if 'symbol' not in hist.columns and 'ticker' in hist.columns:
                        hist['symbol'] = hist['ticker']
                    # 过滤当日
                    hist = hist[hist['trade_date_x'] == trade_date]
                    if hist.empty:
                        continue
                    # 字段映射
                    rename_map = {
                        'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close',
                        'volume': 'vol', 'adjclose': 'adj_close'
                    }
                    for c_from, c_to in list(rename_map.items()):
                        if c_from in hist.columns and c_to not in hist.columns:
                            hist[c_to] = hist[c_from]
                    # 映射回 ts_code
                    hist['ts_code'] = hist['symbol'].map(y2ts)
                    hist['amount'] = None
                    keep_cols = ['ts_code','trade_date_x','open','high','low','close','vol','amount','adj_close']
                    sub_rows = hist.dropna(subset=['ts_code'])[keep_cols]
                    if not sub_rows.empty:
                        rows.append(sub_rows)
                except Exception:
                    continue
            if len(rows) > 0:
                return pandas.concat(rows, ignore_index=True)
    except Exception:
        pass
    # 若所有来源均失败：返回 None（不中断主流程，跳过该日）
    return None

def dump_astock_data():
    sqlEngine = create_engine('mysql+pymysql://root:@127.0.0.1/investment_data', pool_recycle=3600)
    dbConnection = sqlEngine.raw_connection()

    sql = """
    select max(tradedate) as tradedate
        FROM
        (select tradedate, count(tradedate) as symbol_count 
        FROM ts_a_stock_eod_price 
        where tradedate > "2023-05-01" 
        group by tradedate) tradedate_record
    WHERE symbol_count > 1000
    """
    latest_trade_date = pandas.read_sql(sql, dbConnection)["tradedate"][0].strftime('%Y%m%d')
    end_date = datetime.datetime.now().strftime('%Y%m%d')

    trade_date_df = get_trade_cal(latest_trade_date, end_date)
    # Sort from small to big, so that we process earlier date first
    trade_date_df = trade_date_df.sort_values("cal_date")
    for row in trade_date_df.values.tolist():
        trade_date = row[0]
        if trade_date == latest_trade_date:
            continue
        print("Downloading", trade_date)
        ts_data = get_daily(trade_date)
        if ts_data is None:
            continue
        if ts_data.empty:
            continue
        column_mapping = {
            "trade_date_x": "tradedate",
            "high": "high",
            "low": "low",
            "open": "open",
            "close": "close",
            "adj_close": "adjclose",
            "vol": "volume",
            "amount": "amount",
            "ts_code": "symbol"
        }
        data = ts_data.rename(columns=column_mapping)[list(column_mapping.values())]      
        record_num = data.to_sql("ts_a_stock_eod_price", sqlEngine, if_exists='append', index=False)
        print(f"{trade_date} Updated: {record_num} records")

if __name__ == '__main__':
    fire.Fire(dump_astock_data)
