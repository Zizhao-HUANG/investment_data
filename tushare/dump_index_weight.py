import tushare as ts
import os
import datetime
import pandas
import fire
import time
import datetime
from typing import Optional

try:
    import akshare as ak
except Exception:
    ak = None
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

index_list = [
    '000905.SH', # csi500
    '399300.SZ', # csi300
    '000906.SH', # csi800
    '000852.SH', # csi1000
    '000985.SH', # csiall
    ]

def dump_index_data(start_date=None, end_date=None, skip_exists=True):
    if not os.path.exists(f"{file_path}/index_weight/"):
        os.makedirs(f"{file_path}/index_weight/")

    for index_name in index_list:
        time_step = datetime.timedelta(days=15)
        if start_date is None:
            index_info = pro_call_with_timeout(pro, 'index_basic', get_timeout_seconds(), ts_code=index_name)
            list_date = index_info["list_date"][0]
            list_date_obj = datetime.datetime.strptime(list_date, '%Y%m%d')
            index_start_date = list_date_obj
        else:
            index_start_date = datetime.datetime.strptime(str(start_date), '%Y%m%d')
        
        if end_date is None:
            index_end_date = index_start_date + time_step
        else:
            index_end_date = datetime.datetime.strptime(str(end_date), '%Y%m%d')

        filename = f'{file_path}/index_weight/{index_name}.csv'
        print("Dump to: ", filename)
        result_df_list = []
        while index_end_date < datetime.datetime.now():
            df = None
            try:
                df = pro_call_with_timeout(
                    pro,
                    'index_weight',
                    get_timeout_seconds(),
                    index_code=index_name,
                    start_date=index_start_date.strftime('%Y%m%d'),
                    end_date=index_end_date.strftime('%Y%m%d')
                )
            except Exception as e:
                print("[WARN] Tushare index_weight failed, will try AKShare fallback:", e)
            if (df is None) or df.empty:
                # AKShare 指数成分（若可用）
                if ak is not None:
                    try:
                        # akshare 未提供历史权重全量接口；尽可能获取当前或近段时间的权重信息
                        # 这里退化为获取当期成分及权重（若可用），否则跳过
                        code, exch = index_name.split('.')
                        # 使用 stock_zh_index_weight_csindex 获取中证指数（需要指数代码，如 000905）
                        ak_code = code
                        ak_df = ak.stock_zh_index_weight_csindex(symbol=ak_code)
                        if ak_df is not None and not ak_df.empty:
                            # 标准化为 ts 字段 superset
                            ak_df = ak_df.rename(columns={
                                '指数代码': 'index_code',
                                '证券代码': 'con_code',
                                '权重(%)': 'weight'
                            })
                            ak_df['index_code'] = index_name
                            # 近似赋值日期范围为当前窗口尾日
                            ak_df['trade_date'] = index_end_date.strftime('%Y%m%d')
                            result_df_list.append(ak_df[['index_code','con_code','weight','trade_date']])
                    except Exception:
                        pass
            else:
                result_df_list.append(df)
            index_start_date += time_step
            index_end_date += time_step
            time.sleep(0.5)
        if len(result_df_list) == 0:
            continue
        result_df = pandas.concat(result_df_list)
        result_df["stock_code"] = result_df["con_code"]
        result_df.to_csv(filename, index=False)

if __name__ == '__main__':
    fire.Fire(dump_index_data)
